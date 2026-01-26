use anyhow::{anyhow, Result};
use std::collections::HashMap;
use uuid::Uuid;

use crate::graph_utils::graph::{GraphDatabase, Node, Relationship};
use super::query_interface::QueryResultRow;

// NOTE: This is a pragmatic Cypher parser/executor focused on common forms:
// - MATCH (n:Label {k:"v"}), (m:Label) [WHERE ...] RETURN n [, m [, n.prop ...]]
// - MATCH (a:Label)-[r:TYPE]->(b:Label) RETURN a, r, b
// - CREATE (n:Label { ... }) [RETURN n]
// - MERGE (a)-[:TYPE]->(b) with a/b bound by preceding MATCH
// It is not a complete implementation of OpenCypher.

#[derive(Debug, Clone)]
enum Expr {
    Var(String),
    Prop(Box<Expr>, String),
    FuncId(String),
    Str(String),
}

#[derive(Debug, Clone, Default)]
struct NodePattern {
    var: Option<String>,
    label: Option<String>,
    props: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
struct RelPattern {
    var: Option<String>,
    typ: Option<String>,
    // direction: true if ->, false if <-, None for undirected (not supported yet)
    right: bool,
    props: HashMap<String, String>,
    // Variable-length specification (if present): min..=max hops. None => exactly 1 hop
    min_len: Option<usize>,
    max_len: Option<usize>,
}

#[derive(Debug, Clone)]
enum Pattern {
    Node(NodePattern),
    Path { left: NodePattern, rel: RelPattern, right: NodePattern },
}

#[derive(Debug, Clone)]
enum Clause {
    Match { optional: bool, patterns: Vec<Pattern> },
    Where(String), // raw, limited support
    Return { items: Vec<Expr>, distinct: bool, order_by: Vec<(Expr, bool)>, skip: Option<usize>, limit: Option<usize> },
    With { items: Vec<Expr>, distinct: bool, order_by: Vec<(Expr, bool)>, skip: Option<usize>, limit: Option<usize> },
    Create { patterns: Vec<Pattern> },
    Merge { pattern: Pattern },
    Delete { vars: Vec<String>, detach: bool },
    Set { items: Vec<String> },
    Remove { items: Vec<String> },
}

// Find a clause keyword at a token boundary (start or preceded by whitespace) and
// followed by end-of-string or whitespace. Case-insensitive: caller should pass
// an uppercased haystack and uppercase keyword. Returns the byte index in haystack.
fn find_keyword_boundary(hay_up: &str, kw_up: &str) -> Option<usize> {
    let bytes = hay_up.as_bytes();
    let kwb = kw_up.as_bytes();
    if kwb.is_empty() { return None; }
    let n = bytes.len();
    let m = kwb.len();
    if m > n { return None; }
    let mut i = 0;
    while i + m <= n {
        // boundary at start or previous is whitespace
        let prev_ok = if i == 0 { true } else { bytes[i-1].is_ascii_whitespace() };
        if prev_ok && &bytes[i..i+m] == kwb {
            // next boundary ok (end or whitespace)
            let next_ok = if i + m >= n { true } else { bytes[i+m].is_ascii_whitespace() };
            if next_ok { return Some(i); }
        }
        i += 1;
    }
    None
}

fn trim_quotes(s: &str) -> String {
    let t = s.trim();
    if (t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')) {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

fn parse_props(block: &str) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    let inner = block.trim();
    if inner.is_empty() { return Ok(map); }
    for part in inner.split(',') {
        let kv = part.splitn(2, ':').collect::<Vec<_>>();
        if kv.len() != 2 { return Err(anyhow!("invalid property: {}", part)); }
        map.insert(kv[0].trim().to_string(), trim_quotes(kv[1].trim()));
    }
    Ok(map)
}

fn parse_node_pattern(s: &str) -> Result<NodePattern> {
    // (var:Label {k:"v"}) | (:Label) | (var)
    if !s.starts_with('(') || !s.ends_with(')') { return Err(anyhow!("invalid node pattern: {}", s)); }
    let inner = &s[1..s.len()-1];
    let mut np = NodePattern::default();
    // split off props if any
    let (body, props) = if let Some(b) = inner.find('{') {
        let e = inner.rfind('}').ok_or_else(|| anyhow!("unclosed properties"))?;
        (&inner[..b], Some(&inner[b+1..e]))
    } else { (inner, None) };

    // var and label
    let body = body.trim();
    if body.is_empty() {
        // anonymous
    } else if let Some(col) = body.find(':') {
        // Standard openCypher form: (var:Label)
        let v = body[..col].trim();
        if !v.is_empty() { np.var = Some(v.to_string()); }
        let lab = body[col+1..].trim();
        if !lab.is_empty() { np.label = Some(lab.to_string()); }
    } else if let Some(dot) = body.find('.') {
        // Support alternative shorthand form: (var.Label)
        let v = body[..dot].trim();
        let lab = body[dot+1..].trim();
        if !v.is_empty() { np.var = Some(v.to_string()); }
        if !lab.is_empty() { np.label = Some(lab.to_string()); }
    } else {
        // Only variable name
        np.var = Some(body.to_string());
    }
    if let Some(p) = props { np.props = parse_props(p)?; }
    Ok(np)
}

fn parse_rel_pattern(s: &str) -> Result<RelPattern> {
    // -[r:TYPE {k:"v"}]-> or -[:TYPE]-> or -[r]-> or undirected -(r)-
    if !s.starts_with("-[") || !s.ends_with("]-") && !s.ends_with("]->") && !s.ends_with("-]") { return Err(anyhow!("invalid rel pattern: {}", s)); }
    let right = s.ends_with("]->");
    let mid = &s[2..s.len()- if right { 3 } else { 2 }];
    let mut rp = RelPattern { var: None, typ: None, right, props: HashMap::new(), min_len: None, max_len: None };
    let rest = mid.trim();
    // Split off props if present
    let (before_props, props_block) = if let Some(b) = rest.find('{') {
        let e = rest.rfind('}').ok_or_else(|| anyhow!("unclosed relationship properties"))?;
        (&rest[..b], Some(&rest[b+1..e]))
    } else { (rest, None) };
    // Split off variable-length suffix like *3 or *1..3 or *..3 or *1..
    let (head, range_part) = if let Some(star) = before_props.rfind('*') {
        // Only treat as range if '*' appears after any ':' type spec, not at beginning
        let head = before_props[..star].trim();
        let rng = before_props[star+1..].trim();
        if !rng.is_empty() {
            Some((head, rng))
        } else {
            Some((head, ""))
        }
    } else { None }
    .map(|(h, r)| (h, Some(r)))
    .unwrap_or((before_props, None));

    if let Some(col) = head.find(':') {
        let v = head[..col].trim();
        if !v.is_empty() { rp.var = Some(v.to_string()); }
        let t = head[col+1..].trim();
        if !t.is_empty() { rp.typ = Some(t.to_string()); }
    } else if !head.is_empty() {
        rp.var = Some(head.to_string());
    }

    // parse range if present
    if let Some(rng) = range_part {
        if !rng.is_empty() {
            // forms: N | min..max | ..max | min.. | (empty -> treat as 1..MAX)
            if rng.contains("..") {
                let parts: Vec<&str> = rng.split("..").collect();
                if parts.len() != 2 { return Err(anyhow!("invalid variable-length range: *{}", rng)); }
                let min = if parts[0].trim().is_empty() { None } else { Some(parts[0].trim().parse::<usize>().map_err(|_| anyhow!("invalid min in *{}", rng))?) };
                let max = if parts[1].trim().is_empty() { None } else { Some(parts[1].trim().parse::<usize>().map_err(|_| anyhow!("invalid max in *{}", rng))?) };
                rp.min_len = min;
                rp.max_len = max;
            } else {
                // single number
                let n = rng.parse::<usize>().map_err(|_| anyhow!("invalid length in *{}", rng))?;
                rp.min_len = Some(n);
                rp.max_len = Some(n);
            }
        } else {
            // bare * means 1..=INF (we will cap later)
            rp.min_len = Some(1);
            rp.max_len = None;
        }
    }
    if let Some(p) = props_block { rp.props = parse_props(p)?; }
    Ok(rp)
}

fn split_top_level_comma(s: &str) -> Vec<String> {
    // naive split by commas not inside braces
    let mut out = Vec::new();
    let mut level = 0i32;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b as char {
            '{' => level += 1,
            '}' => level -= 1,
            ',' if level == 0 => {
                out.push(s[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(s[start..].trim().to_string());
    out
}

fn parse_pattern(s: &str) -> Result<Pattern> {
    let mut s = s.trim();
    // Defensive: if a node pattern is followed by a clause on the same string (e.g., due to upstream splitting),
    // truncate at the first closing ')' to keep just the node pattern.
    // This helps for inputs like "(m:Movie)\nWHERE ..." accidentally passed as one pattern string.
    if s.starts_with('(') {
        if let Some(pidx) = s.find(')') {
            // If there appears to be clause text after the node, drop it
            let after = s[pidx+1..].to_uppercase();
            if after.contains("WHERE") || after.contains("RETURN") || after.contains("DELETE") || after.contains("DETACH DELETE") || after.contains("CREATE") || after.contains("MERGE") || after.contains("SET ") || after.contains("REMOVE ") {
                s = &s[..=pidx];
            }
        }
    }
    // path like (a:Label)-[r:TYPE]->(b:Label) or undirected (a)-[r]-(b)
    if let Some(mid_start) = s.find("-[") {
        // Find the end of the left node by locating the last ')' before the rel start
        let left_end = s[..mid_start]
            .rfind(')')
            .ok_or_else(|| anyhow!("bad path left"))?;
        let left = &s[..=left_end];

        // From the rel start, find the closing ']' of the relationship spec
        let after_rel_bracket = s[mid_start..]
            .find(']')
            .map(|k| mid_start + k)
            .ok_or_else(|| anyhow!("bad relationship pattern (no closing ]): {}", s))?;

        // Determine direction by looking at chars after ']'
        // Expect either "]->(" or "]-(" (we will locate the '(' explicitly next)
        let after_br = after_rel_bracket + 1;
        let right_dir = s.get(after_br..after_br + 2).map(|t| t == "->").unwrap_or(false);

        // Locate the start of the right node pattern: the next '(' after ']' (skipping '-' or '>' if present)
        let right_paren_idx = s[after_br..]
            .find('(')
            .map(|k| after_br + k)
            .ok_or_else(|| anyhow!("bad path right (no right node)") )?;

        // Relationship slice is between mid_start and the start of right node
        let rel_slice = &s[mid_start..right_paren_idx];
        let right = &s[right_paren_idx..];

        let mut rp = parse_rel_pattern(rel_slice)?;
        // Ensure the direction flag matches what we detected
        rp.right = right_dir;

        let np_left = parse_node_pattern(left)?;
        let np_right = parse_node_pattern(right)?;
        Ok(Pattern::Path { left: np_left, rel: rp, right: np_right })
    } else {
        Ok(Pattern::Node(parse_node_pattern(s)?))
    }
}

fn parse_return_items(s: &str) -> Result<Vec<Expr>> {
    let mut items = Vec::new();
    for part in s.split(',') {
        let p = part.trim();
        if p.to_uppercase().starts_with("ID(") && p.ends_with(')') {
            let v = p[3..p.len()-1].trim();
            items.push(Expr::FuncId(v.to_string()));
        } else if let Some(dot) = p.find('.') {
            let v = p[..dot].trim().to_string();
            let prop = p[dot+1..].trim().to_string();
            items.push(Expr::Prop(Box::new(Expr::Var(v)), prop));
        } else if p.starts_with('"') || p.starts_with('\'') { 
            items.push(Expr::Str(trim_quotes(p)));
        } else {
            items.push(Expr::Var(p.to_string()));
        }
    }
    Ok(items)
}

fn parse_order_by(s: &str) -> Result<Vec<(Expr, bool)>> {
    // returns list of (expr, asc=true/false)
    let mut out = Vec::new();
    for part in s.split(',') {
        let p = part.trim();
        let mut asc = true;
        let pu = p.to_uppercase();
        let (expr_str, dir_part) = if let Some(idx) = pu.rfind(" DESC") {
            if idx + 5 == p.len() || p[idx+5..].trim().is_empty() {
                asc = false;
                (&p[..idx], Some("DESC"))
            } else { (&p[..], None) }
        } else if let Some(idx) = pu.rfind(" ASC") {
            if idx + 4 == p.len() || p[idx+4..].trim().is_empty() {
                asc = true;
                (&p[..idx], Some("ASC"))
            } else { (&p[..], None) }
        } else { (&p[..], None) };
        let expr = if expr_str.to_uppercase().starts_with("ID(") && expr_str.ends_with(')') {
            let v = expr_str[3..expr_str.len()-1].trim();
            Expr::FuncId(v.to_string())
        } else if let Some(dot) = expr_str.find('.') {
            let v = expr_str[..dot].trim().to_string();
            let prop = expr_str[dot+1..].trim().to_string();
            Expr::Prop(Box::new(Expr::Var(v)), prop)
        } else {
            Expr::Var(expr_str.trim().to_string())
        };
        let _ = dir_part; // not used beyond detection
        out.push((expr, asc));
    }
    Ok(out)
}

fn parse(query: &str) -> Result<Vec<Clause>> {
    // Very small parser: MATCH ... [WHERE ...] RETURN ... | CREATE ... [RETURN ...] | MERGE ...
    let q = query.trim();
    let mut clauses = Vec::new();
    let up = q.to_uppercase();
    if up.starts_with("MATCH ") || up.starts_with("OPTIONAL MATCH ") {
        let optional = up.starts_with("OPTIONAL MATCH ");
        let pstart = if optional { 15 } else { 6 };
        // find RETURN or WHERE or end
        let rest = q[pstart..].trim();
        let mut where_part: Option<&str> = None;
        // Defer SET/REMOVE so they execute AFTER MATCH/WHERE
        let mut deferred_set: Option<Vec<String>> = None;
        let mut deferred_remove: Option<Vec<String>> = None;
        let rest_up = rest.to_uppercase();
        let (mut patterns_str, tail) = if let Some(i) = find_keyword_boundary(&rest_up, "RETURN") {
            // There is a RETURN later; but there may also be WHERE/SET/REMOVE before it.
            let head = &rest[..i];
            let head_up = head.to_uppercase();
            // Determine earliest clause (WHERE/SET/REMOVE) position to cut patterns region
            let where_pos = find_keyword_boundary(&head_up, "WHERE");
            let set_pos = find_keyword_boundary(&head_up, "SET");
            let rem_pos = find_keyword_boundary(&head_up, "REMOVE");
            let mut cut_idx = head.len();
            for p in [where_pos, set_pos, rem_pos].into_iter().flatten() { if p < cut_idx { cut_idx = p; } }
            let patterns_str = &head[..cut_idx];
            // Extract WHERE if present
            if let Some(wi) = where_pos {
                let after_kw = &head[wi..];
                let w_body = after_kw.strip_prefix("WHERE").map(|s| s.trim_start()).unwrap_or(after_kw);
                // If SET/REMOVE also exist after WHERE within head, truncate WHERE body accordingly
                let w_up = w_body.to_uppercase();
                let w_trimmed = if let Some(si) = find_keyword_boundary(&w_up, "SET") {
                    &w_body[..si]
                } else if let Some(ri) = find_keyword_boundary(&w_up, "REMOVE") {
                    &w_body[..ri]
                } else { w_body };
                where_part = Some(w_trimmed.trim());
            }
            // Extract SET if present (prior to RETURN) — defer execution
            if let Some(si) = set_pos {
                let after_kw = &head[si..];
                let s_body = after_kw.strip_prefix("SET").map(|s| s.trim_start()).unwrap_or(after_kw);
                // Truncate at REMOVE if it appears after SET in head
                let sb_up = s_body.to_uppercase();
                let s_items_str = if let Some(ri) = find_keyword_boundary(&sb_up, "REMOVE") { &s_body[..ri] } else { s_body };
                let items = split_top_level_comma(s_items_str);
                deferred_set = Some(items);
            }
            // Extract REMOVE if present (prior to RETURN) — defer execution
            if let Some(ri) = rem_pos {
                let after_kw = &head[ri..];
                let r_body = after_kw.strip_prefix("REMOVE").map(|s| s.trim_start()).unwrap_or(after_kw);
                let items = split_top_level_comma(r_body);
                deferred_remove = Some(items);
            }
            (patterns_str, Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "WHERE") {
            let patterns_str = &rest[..i];
            // "WHERE" is 5 chars; skip keyword and following space if present
            let after_kw = &rest[i..];
            let w_and_tail = after_kw.strip_prefix("WHERE").map(|s| s.trim_start()).unwrap_or(after_kw);
            // check if there is RETURN after where
            let w_up = w_and_tail.to_uppercase();
            if let Some(k) = find_keyword_boundary(&w_up, "RETURN") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "SET") {
                // Defer SET then continue parsing tail after it
                let set_items = split_top_level_comma(&w_and_tail[..k]);
                deferred_set = Some(set_items);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "REMOVE") {
                let rem_items = split_top_level_comma(&w_and_tail[..k]);
                deferred_remove = Some(rem_items);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "MERGE") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "CREATE") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "DETACH DELETE") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "DELETE") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else {
                where_part = Some(w_and_tail);
                (patterns_str, None)
            }
        } else if let Some(i) = find_keyword_boundary(&rest_up, "MERGE") {
            (&rest[..i], Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "CREATE") {
            (&rest[..i], Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "WITH") {
            (&rest[..i], Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "SET") {
            // Defer SET that appears between MATCH and end
            let head = &rest[..i];
            let after_kw = &rest[i..];
            let s_body = after_kw.strip_prefix("SET").map(|s| s.trim_start()).unwrap_or(after_kw);
            // Truncate at REMOVE if it appears after SET
            let sb_up = s_body.to_uppercase();
            let s_items_str = if let Some(ri) = find_keyword_boundary(&sb_up, "REMOVE") { &s_body[..ri] } else { s_body };
            deferred_set = Some(split_top_level_comma(s_items_str));
            (head, Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "REMOVE") {
            let head = &rest[..i];
            let after_kw = &rest[i..];
            let r_body = after_kw.strip_prefix("REMOVE").map(|s| s.trim_start()).unwrap_or(after_kw);
            deferred_remove = Some(split_top_level_comma(r_body));
            (head, Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "DETACH DELETE") {
            (&rest[..i], Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "DELETE") {
            (&rest[..i], Some(&rest[i..]))
        } else { (rest, None) };

        // Fallback: if no WHERE/RETURN tail detected but the text still contains a WHERE token
        // (e.g., due to unusual whitespace/newline placement), split on the first "WHERE" occurrence.
        if tail.is_none() {
            if let Some(i) = rest_up.find("WHERE") {
                where_part = Some(rest[i+5..].trim());
                patterns_str = &rest[..i];
            }
        }

        // Defensive: if patterns_str accidentally contains trailing clause text (SET/REMOVE/RETURN/DELETE),
        // truncate at the earliest occurrence before splitting by commas.
        let pat_up = patterns_str.to_uppercase();
        let mut cut = patterns_str.len();
        for kw in [" DETACH DELETE ", " DELETE ", " RETURN ", " SET ", " REMOVE "] {
            if let Some(i) = pat_up.find(kw) { if i < cut { cut = i; } }
        }
        let patterns_region = &patterns_str[..cut];
        let mut patterns = Vec::new();
        for pat in split_top_level_comma(patterns_region) { if !pat.is_empty() { patterns.push(parse_pattern(&pat)?); } }
        clauses.push(Clause::Match { optional, patterns });
        if let Some(w) = where_part { clauses.push(Clause::Where(w.trim().to_string())); }
        if let Some(items) = deferred_set.take() { clauses.push(Clause::Set { items }); }
        if let Some(items) = deferred_remove.take() { clauses.push(Clause::Remove { items }); }
        if let Some(t) = tail {
            let t = t.trim();
            let tup = t.to_uppercase();
            if tup.starts_with("RETURN ") {
                // Support RETURN [DISTINCT] ... [ORDER BY ...] [SKIP n] [LIMIT n]
                let mut body = t[7..].trim();
                let mut distinct = false;
                let bu = body.to_uppercase();
                if bu.starts_with("DISTINCT ") {
                    distinct = true;
                    body = body[9..].trim();
                }
                let _body_up = body.to_uppercase();
                // Extract LIMIT and SKIP from the end if present (order-insensitive between them)
                let mut limit: Option<usize> = None;
                let mut skip: Option<usize> = None;
                // We'll iteratively peel off from the end
                let mut working = body.to_string();
                loop {
                    let up = working.to_uppercase();
                    if let Some(idx) = up.rfind(" LIMIT ") {
                        let tail = working[idx+7..].trim();
                        if let Some(_space) = tail.find(' ') { /* keep only last segment */ }
                        if let Ok(n) = tail.parse::<usize>() { limit = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                    }
                    if let Some(idx) = up.rfind(" SKIP ") {
                        let tail = working[idx+6..].trim();
                        if let Ok(n) = tail.parse::<usize>() { skip = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                    }
                    break;
                }
                // Extract ORDER BY if present
                let mut order_by: Vec<(Expr, bool)> = Vec::new();
                let up2 = working.to_uppercase();
                let (items_part, order_part_opt) = if let Some(i) = up2.rfind(" ORDER BY ") {
                    (&working[..i], Some(&working[i+10..]))
                } else { (&working[..], None) };
                if let Some(op) = order_part_opt { order_by = parse_order_by(op.trim())?; }
                let items = parse_return_items(items_part.trim())?;
                clauses.push(Clause::Return { items, distinct, order_by, skip, limit });
            } else if tup.starts_with("WITH ") {
                // Parse WITH ... [ORDER BY ...] [SKIP n] [LIMIT n] [RETURN ...]
                let mut body = t[5..].trim();
                let mut distinct = false;
                let bu = body.to_uppercase();
                if bu.starts_with("DISTINCT ") {
                    distinct = true;
                    body = body[9..].trim();
                }
                // We also allow a RETURN after WITH; split it off first from the end to keep ORDER/SKIP/LIMIT parsing intact
                let mut trailing_return: Option<&str> = None;
                let upb = body.to_uppercase();
                if let Some(i) = find_keyword_boundary(&upb, "RETURN") {
                    trailing_return = Some(&body[i..]);
                    body = body[..i].trim();
                }
                // Now parse ORDER BY / SKIP / LIMIT like RETURN
                let mut limit: Option<usize> = None;
                let mut skip: Option<usize> = None;
                let mut working = body.to_string();
                loop {
                    let up = working.to_uppercase();
                    if let Some(idx) = up.rfind(" LIMIT ") {
                        let tail = working[idx+7..].trim();
                        if let Ok(n) = tail.parse::<usize>() { limit = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                    }
                    if let Some(idx) = up.rfind(" SKIP ") {
                        let tail = working[idx+6..].trim();
                        if let Ok(n) = tail.parse::<usize>() { skip = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                    }
                    break;
                }
                let mut order_by: Vec<(Expr, bool)> = Vec::new();
                let up2 = working.to_uppercase();
                let (items_part, order_part_opt) = if let Some(i) = up2.rfind(" ORDER BY ") {
                    (&working[..i], Some(&working[i+10..]))
                } else { (&working[..], None) };
                if let Some(op) = order_part_opt { order_by = parse_order_by(op.trim())?; }
                let items = parse_return_items(items_part.trim())?;
                clauses.push(Clause::With { items, distinct, order_by, skip, limit });
                // If there is a trailing RETURN, parse it as well
                if let Some(ret) = trailing_return {
                    let mut body = ret[6..].trim(); // after RETURN
                    let mut distinct_r = false;
                    let bu = body.to_uppercase();
                    if bu.starts_with("DISTINCT ") {
                        distinct_r = true;
                        body = body[9..].trim();
                    }
                    // Parse SKIP/LIMIT at end, ORDER BY, then items
                    let mut limit: Option<usize> = None;
                    let mut skip: Option<usize> = None;
                    let mut working = body.to_string();
                    loop {
                        let up = working.to_uppercase();
                        if let Some(idx) = up.rfind(" LIMIT ") {
                            let tail = working[idx+7..].trim();
                            if let Ok(n) = tail.parse::<usize>() { limit = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                        }
                        if let Some(idx) = up.rfind(" SKIP ") {
                            let tail = working[idx+6..].trim();
                            if let Ok(n) = tail.parse::<usize>() { skip = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                        }
                        break;
                    }
                    let mut order_by: Vec<(Expr, bool)> = Vec::new();
                    let up2 = working.to_uppercase();
                    let (items_part, order_part_opt) = if let Some(i) = up2.rfind(" ORDER BY ") {
                        (&working[..i], Some(&working[i+10..]))
                    } else { (&working[..], None) };
                    if let Some(op) = order_part_opt { order_by = parse_order_by(op.trim())?; }
                    let items = parse_return_items(items_part.trim())?;
                    clauses.push(Clause::Return { items, distinct: distinct_r, order_by, skip, limit });
                }
            } else if tup.starts_with("CREATE ") {
                let pats = &t[6..].trim();
                let mut patterns = Vec::new();
                for pat in split_top_level_comma(pats) { if !pat.is_empty() { patterns.push(parse_pattern(&pat)?); } }
                clauses.push(Clause::Create { patterns });
            } else if tup.starts_with("MERGE ") {
                let body = &t[6..].trim();
                let pattern = parse_pattern(body)?;
                clauses.push(Clause::Merge { pattern });
            } else if tup.starts_with("DELETE ") {
                let vars_str = &t[7..];
                let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
                clauses.push(Clause::Delete { vars, detach: false });
            } else if tup.starts_with("DETACH DELETE ") {
                let vars_str = &t[14..];
                let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
                clauses.push(Clause::Delete { vars, detach: true });
            } else if tup.starts_with("SET ") {
                // Allow SET ... followed by RETURN ...
                let body = &t[4..].trim();
                let upb = body.to_uppercase();
                if let Some(i) = upb.find(" RETURN ") {
                    let items_str = &body[..i];
                    let items = split_top_level_comma(items_str);
                    clauses.push(Clause::Set { items });
                    let ret_part = &body[i+8..];
                    let items = parse_return_items(ret_part.trim())?;
                    clauses.push(Clause::Return { items, distinct: false, order_by: Vec::new(), skip: None, limit: None });
                } else {
                    let items = split_top_level_comma(body);
                    clauses.push(Clause::Set { items });
                }
            } else if tup.starts_with("REMOVE ") {
                // Allow REMOVE ... followed by RETURN ...
                let body = &t[7..].trim();
                let upb = body.to_uppercase();
                if let Some(i) = upb.find(" RETURN ") {
                    let items_str = &body[..i];
                    let items = split_top_level_comma(items_str);
                    clauses.push(Clause::Remove { items });
                    let ret_part = &body[i+8..];
                    let items = parse_return_items(ret_part.trim())?;
                    clauses.push(Clause::Return { items, distinct: false, order_by: Vec::new(), skip: None, limit: None });
                } else {
                    let items = split_top_level_comma(body);
                    clauses.push(Clause::Remove { items });
                }
            }
        }
        return Ok(clauses);
    } else if up.starts_with("CREATE") {
        // Support CREATE followed by any whitespace/newlines before patterns
        let body = &q[6..].trim();
        let mut parts = body.splitn(2, " RETURN ");
        let pats = match parts.next() {
            Some(s) => s,
            None => return Err(anyhow!("missing CREATE patterns")),
        };
        let mut patterns = Vec::new();
        for pat in split_top_level_comma(pats) { if !pat.is_empty() { patterns.push(parse_pattern(&pat)?); } }
        clauses.push(Clause::Create { patterns });
        if let Some(ret) = parts.next() {
            // Allow ORDER BY/LIMIT/SKIP after RETURN even in CREATE ... RETURN
            let ret_trim = ret.trim();
            let body = ret_trim;
            let mut limit: Option<usize> = None;
            let mut skip: Option<usize> = None;
            let mut working = body.to_string();
            loop {
                let up = working.to_uppercase();
                if let Some(idx) = up.rfind(" LIMIT ") {
                    let tail = working[idx+7..].trim();
                    if let Ok(n) = tail.parse::<usize>() { limit = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                }
                if let Some(idx) = up.rfind(" SKIP ") {
                    let tail = working[idx+6..].trim();
                    if let Ok(n) = tail.parse::<usize>() { skip = Some(n); working = working[..idx].trim_end().to_string(); continue; }
                }
                break;
            }
            let up2 = working.to_uppercase();
            let (items_part, order_part_opt) = if let Some(i) = up2.rfind(" ORDER BY ") {
                (&working[..i], Some(&working[i+10..]))
            } else { (&working[..], None) };
            let mut order_by: Vec<(Expr, bool)> = Vec::new();
            if let Some(op) = order_part_opt { order_by = parse_order_by(op.trim())?; }
            let items = parse_return_items(items_part.trim())?;
            clauses.push(Clause::Return { items, distinct: false, order_by, skip, limit });
        }
        return Ok(clauses);
    } else if up.starts_with("MERGE ") {
        let body = &q[6..].trim();
        let pattern = parse_pattern(body)?;
        clauses.push(Clause::Merge { pattern });
        return Ok(clauses);
    } else if up.starts_with("WITH ") {
        // Standalone WITH at statement start
        // Parse WITH ... [ORDER BY ...] [SKIP n] [LIMIT n]
        let body = &q[5..].trim();
        // No trailing RETURN handled here (next statement may contain it)
        let mut limit: Option<usize> = None;
        let mut skip: Option<usize> = None;
        let mut working = body.to_string();
        loop {
            let up = working.to_uppercase();
            if let Some(idx) = up.rfind(" LIMIT ") {
                let tail = working[idx+7..].trim();
                if let Ok(n) = tail.parse::<usize>() { limit = Some(n); working = working[..idx].trim_end().to_string(); continue; }
            }
            if let Some(idx) = up.rfind(" SKIP ") {
                let tail = working[idx+6..].trim();
                if let Ok(n) = tail.parse::<usize>() { skip = Some(n); working = working[..idx].trim_end().to_string(); continue; }
            }
            break;
        }
        let mut order_by: Vec<(Expr, bool)> = Vec::new();
        let up2 = working.to_uppercase();
        let (items_part, order_part_opt) = if let Some(i) = up2.rfind(" ORDER BY ") {
            (&working[..i], Some(&working[i+10..]))
        } else { (&working[..], None) };
        if let Some(op) = order_part_opt { order_by = parse_order_by(op.trim())?; }
        let items = parse_return_items(items_part.trim())?;
        clauses.push(Clause::With { items, distinct: false, order_by, skip, limit });
        return Ok(clauses);
    } else if up.starts_with("SET ") {
        let items_str = &q[4..];
        let items = split_top_level_comma(items_str);
        clauses.push(Clause::Set { items });
        return Ok(clauses);
    } else if up.starts_with("REMOVE ") {
        let items_str = &q[7..];
        let items = split_top_level_comma(items_str);
        clauses.push(Clause::Remove { items });
        return Ok(clauses);
    } else if up.starts_with("DELETE ") {
        let vars_str = &q[7..];
        let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
        clauses.push(Clause::Delete { vars, detach: false });
        return Ok(clauses);
    } else if up.starts_with("DETACH DELETE ") {
        let vars_str = &q[14..];
        let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
        clauses.push(Clause::Delete { vars, detach: true });
        return Ok(clauses);
    }
    Err(anyhow!("Unsupported or unrecognized Cypher statement"))
}

fn resolve_param(raw: &str, params: &HashMap<String, String>) -> Result<String> {
    let t = raw.trim();
    if t.starts_with('$') {
        let key = &t[1..];
        params.get(key).cloned().ok_or_else(|| anyhow!("Missing parameter: ${}", key))
    } else {
        Ok(trim_quotes(t))
    }
}

pub fn execute_cypher_with_params(db: &mut GraphDatabase, query: &str, params: &HashMap<String, String>) -> Result<Vec<QueryResultRow>> {
    let clauses = parse(query)?;
    // binding map: var -> either Node or Relationship id
    #[derive(Clone)]
    enum Val { NodeId(Uuid), RelId(Uuid) }
    let mut rows: Vec<HashMap<String, Val>> = vec![HashMap::new()];

    // helpers
    let get_node = |db: &GraphDatabase, id: &Uuid| -> Option<Node> { db.get_node(*id).cloned() };
    let get_rel = |db: &GraphDatabase, id: &Uuid| -> Option<Relationship> { db.get_relationship(*id).cloned() };

    for cl in clauses {
        match cl {
            Clause::Match { optional: _optional, patterns } => {
                let mut next_rows: Vec<HashMap<String, Val>> = Vec::new();
                for row in &rows {
                    // expand each pattern sequentially (AND semantics)
                    let mut partials = vec![row.clone()];
                    for p in &patterns {
                        let mut new_partials: Vec<HashMap<String, Val>> = Vec::new();
                        match p {
                            Pattern::Node(np) => {
                                for (nid, n) in &db.nodes {
                                    if let Some(l) = &np.label { if &n.label != l { continue; } }
                                    // property exact matches
                                    let mut ok = true;
                                    for (k, vraw) in &np.props {
                                        let v = resolve_param(vraw, params)?;
                                        if n.metadata.get(k) != Some(&v) { ok = false; break; }
                                    }
                                    if !ok { continue; }
                                    for part in &partials {
                                        // bind var if present and consistent
                                        let mut m = part.clone();
                                        if let Some(v) = &np.var {
                                            if let Some(prev) = m.get(v) { if !matches!(prev, Val::NodeId(pid) if pid == nid) { continue; } }
                                            m.insert(v.clone(), Val::NodeId(*nid));
                                        }
                                        new_partials.push(m);
                                    }
                                }
                            }
                            Pattern::Path { left, rel, right } => {
                                // Variable-length?
                                if rel.min_len.is_some() || rel.max_len.is_some() {
                                    if !rel.props.is_empty() {
                                        return Err(anyhow!("variable-length relationships with properties not supported yet"));
                                    }
                                    // Determine allowed hop range
                                    let min_hops = rel.min_len.unwrap_or(1);
                                    // Cap open-ended max to avoid infinite traversal
                                    let cap = 8usize; // conservative cap
                                    let max_hops = rel.max_len.unwrap_or(cap).min(cap);

                                    // Helper to test node metadata against NodePattern
                                    let node_ok = |n: &Node, pat: &NodePattern| -> bool {
                                        if let Some(lab) = &pat.label { if &n.label != lab { return false; } }
                                        for (k, vraw) in &pat.props {
                                            // In closures, we cannot use resolve_param easily; patterns here should not include params for variable-length endpoints in our current usage. Fallback to exact.
                                            if n.metadata.get(k) != Some(vraw) { return false; }
                                        }
                                        true
                                    };

                                    // Pre-collect candidate left and right node ids
                                    let mut left_ids: Vec<Uuid> = Vec::new();
                                    let mut right_ids: Vec<Uuid> = Vec::new();
                                    for (nid, n) in &db.nodes { if node_ok(n, left) { left_ids.push(*nid); } }
                                    for (nid, n) in &db.nodes { if node_ok(n, right) { right_ids.push(*nid); } }

                                    // Build adjacency filtered by type and direction
                                    let mut adj_fwd: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
                                    let mut adj_back: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
                                    for (_rid, r) in &db.relationships {
                                        if let Some(t) = &rel.typ { if &r.label != t { continue; } }
                                        adj_fwd.entry(r.from_node).or_default().push(r.to_node);
                                        adj_back.entry(r.to_node).or_default().push(r.from_node);
                                    }

                                    // For each partial row, expand combinations
                                    for part in &partials {
                                        for &lid in &left_ids {
                                            // Check existing binding consistency for left.var
                                            if let Some(v) = &left.var { if let Some(prev) = part.get(v) { if !matches!(prev, Val::NodeId(pid) if *pid == lid) { continue; } } }

                                            // BFS limited by hop bounds
                                            use std::collections::{VecDeque, HashSet};
                                            let mut qd: VecDeque<(Uuid, usize)> = VecDeque::new();
                                            let mut seen: HashSet<Uuid> = HashSet::new();
                                            qd.push_back((lid, 0));
                                            seen.insert(lid);

                                            while let Some((cur, d)) = qd.pop_front() {
                                                if d >= min_hops && d <= max_hops {
                                                    // candidates that match right pattern
                                                    if right_ids.contains(&cur) {
                                                        // Direction handling: if rel.right true, we already used fwd adjacency; if false (undirected), both were built
                                                        let mut m = part.clone();
                                                        if let Some(v) = &left.var { m.insert(v.clone(), Val::NodeId(lid)); }
                                                        if let Some(v) = &right.var { m.insert(v.clone(), Val::NodeId(cur)); }
                                                        new_partials.push(m);
                                                    }
                                                }
                                                if d == max_hops { continue; }
                                                // advance
                                                let nexts: &[Uuid] = if rel.right {
                                                    adj_fwd.get(&cur).map(|v| v.as_slice()).unwrap_or(&[])
                                                } else {
                                                    // undirected: union of fwd and back
                                                    // Build a temporary vector
                                                    let mut tmp: Vec<Uuid> = Vec::new();
                                                    if let Some(v) = adj_fwd.get(&cur) { tmp.extend_from_slice(v); }
                                                    if let Some(v) = adj_back.get(&cur) { tmp.extend_from_slice(v); }
                                                    // We will enqueue from tmp below
                                                    // To satisfy borrow checker, handle after block
                                                    // Use a marker
                                                    // We'll fall through to custom handling
                                                    // return marker by abusing empty slice path
                                                    &[]
                                                };
                                                if rel.right {
                                                    for &nx in nexts {
                                                        if !seen.contains(&nx) { seen.insert(nx); qd.push_back((nx, d+1)); }
                                                    }
                                                } else {
                                                    // Undirected step: handle both directions
                                                    if let Some(v) = adj_fwd.get(&cur) {
                                                        for &nx in v { if !seen.contains(&nx) { seen.insert(nx); qd.push_back((nx, d+1)); } }
                                                    }
                                                    if let Some(v) = adj_back.get(&cur) {
                                                        for &nx in v { if !seen.contains(&nx) { seen.insert(nx); qd.push_back((nx, d+1)); } }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    // enumerate triples (a)-[r:TYPE]->(b) or undirected (a)-[r]-(b)
                                    for (_rid, r) in &db.relationships {
                                        if let Some(t) = &rel.typ { if &r.label != t { continue; } }
                                        // relationship properties exact match (with param resolution)
                                        let mut ok_rel_props = true;
                                        for (k, vraw) in &rel.props {
                                            let v = resolve_param(vraw, params)?;
                                            if r.metadata.get(k) != Some(&v) { ok_rel_props = false; break; }
                                        }
                                        if !ok_rel_props { continue; }
                                        let (Some(from), Some(to)) = (
                                            db.nodes.get(&r.from_node),
                                            db.nodes.get(&r.to_node),
                                        ) else { continue; };

                                        // Helper to try match given (L,R) node order
                                        let try_match = |left_np: &NodePattern, right_np: &NodePattern, a: &Node, b: &Node| -> bool {
                                            if let Some(lab) = &left_np.label { if &a.label != lab { return false; } }
                                            for (k, vraw) in &left_np.props { if a.metadata.get(k) != Some(vraw) { return false; } }
                                            if let Some(lab) = &right_np.label { if &b.label != lab { return false; } }
                                            for (k, vraw) in &right_np.props { if b.metadata.get(k) != Some(vraw) { return false; } }
                                            true
                                        };

                                        // directed pattern: only from->to
                                        if rel.right {
                                            if !try_match(left, right, from, to) { continue; }
                                            for part in &partials {
                                                let mut m = part.clone();
                                                if let Some(v) = &left.var { if let Some(prev) = m.get(v) { if !matches!(prev, Val::NodeId(pid) if *pid == from.id) { continue; } } m.insert(v.clone(), Val::NodeId(from.id)); }
                                                if let Some(rv) = &rel.var { if let Some(prev) = m.get(rv) { if !matches!(prev, Val::RelId(pid) if *pid == r.id) { continue; } } m.insert(rv.clone(), Val::RelId(r.id)); }
                                                if let Some(v) = &right.var { if let Some(prev) = m.get(v) { if !matches!(prev, Val::NodeId(pid) if *pid == to.id) { continue; } } m.insert(v.clone(), Val::NodeId(to.id)); }
                                                new_partials.push(m);
                                            }
                                        } else {
                                            // undirected: try from->to mapping
                                            if try_match(left, right, from, to) {
                                                for part in &partials {
                                                    let mut m = part.clone();
                                                    if let Some(v) = &left.var { if let Some(prev) = m.get(v) { if !matches!(prev, Val::NodeId(pid) if *pid == from.id) { continue; } } m.insert(v.clone(), Val::NodeId(from.id)); }
                                                    if let Some(rv) = &rel.var { if let Some(prev) = m.get(rv) { if !matches!(prev, Val::RelId(pid) if *pid == r.id) { continue; } } m.insert(rv.clone(), Val::RelId(r.id)); }
                                                    if let Some(v) = &right.var { if let Some(prev) = m.get(v) { if !matches!(prev, Val::NodeId(pid) if *pid == to.id) { continue; } } m.insert(v.clone(), Val::NodeId(to.id)); }
                                                    new_partials.push(m);
                                                }
                                            }
                                            // also try swapped mapping to support -(r)- patterns
                                            if try_match(left, right, to, from) {
                                                for part in &partials {
                                                    let mut m = part.clone();
                                                    if let Some(v) = &left.var { if let Some(prev) = m.get(v) { if !matches!(prev, Val::NodeId(pid) if *pid == to.id) { continue; } } m.insert(v.clone(), Val::NodeId(to.id)); }
                                                    if let Some(rv) = &rel.var { if let Some(prev) = m.get(rv) { if !matches!(prev, Val::RelId(pid) if *pid == r.id) { continue; } } m.insert(rv.clone(), Val::RelId(r.id)); }
                                                    if let Some(v) = &right.var { if let Some(prev) = m.get(v) { if !matches!(prev, Val::NodeId(pid) if *pid == from.id) { continue; } } m.insert(v.clone(), Val::NodeId(from.id)); }
                                                    new_partials.push(m);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        partials = new_partials;
                    }
                    next_rows.extend(partials);
                }
                rows = next_rows;
            }
            Clause::Where(w) => {
                // WHERE support: conjunctive clauses with AND; supports
                // - id(a) <op> id(b)
                // - var.prop <op> literal
                // - var.prop CONTAINS 'substr'
                fn split_where_and(s: &str) -> Vec<String> {
                    let mut out = Vec::new();
                    let mut start = 0usize;
                    let mut i = 0usize;
                    let bytes = s.as_bytes();
                    let n = bytes.len();
                    let mut in_sq = false;
                    let mut in_dq = false;
                    while i < n {
                        let c = bytes[i] as char;
                        if c == '\'' && !in_dq { in_sq = !in_sq; i += 1; continue; }
                        if c == '"' && !in_sq { in_dq = !in_dq; i += 1; continue; }
                        if !in_sq && !in_dq {
                            // check for AND with boundaries
                            if i + 3 <= n {
                                let seg = &s[i..i+3];
                                if seg.eq("AND") || seg.eq_ignore_ascii_case("AND") {
                                    // ensure boundaries are whitespace around
                                    let prev_ws = i == 0 || bytes[i-1].is_ascii_whitespace();
                                    let next_ws = i+3 >= n || bytes[i+3].is_ascii_whitespace();
                                    if prev_ws && next_ws {
                                        out.push(s[start..i].trim().to_string());
                                        start = i+3;
                                        i += 3;
                                        continue;
                                    }
                                }
                            }
                        }
                        i += 1;
                    }
                    out.push(s[start..].trim().to_string());
                    out.retain(|x| !x.is_empty());
                    out
                }

                fn trim_quotes_owned(s: &str) -> String { trim_quotes(s) }

                fn parse_id_compare(expr: &str) -> Option<(String, String, String)> {
                    let mut s = expr.trim().to_string();
                    s = s.replace('\n', " ");
                    s = s.split_whitespace().collect::<Vec<_>>().join(" ");
                    let s = s.replace(' ', "");
                    let ops = ["<=", ">=", "<>", "<", ">", "="];
                    for op in ops {
                        if let Some(i) = s.find(op) {
                            let lhs = &s[..i];
                            let rhs = &s[i+op.len()..];
                            if lhs.starts_with("id(") && lhs.ends_with(")") && rhs.starts_with("id(") && rhs.ends_with(")") {
                                let lv = lhs[3..lhs.len()-1].to_string();
                                let rv = rhs[3..rhs.len()-1].to_string();
                                return Some((lv, op.to_string(), rv));
                            }
                        }
                    }
                    None
                }

                fn parse_var_prop_comp(expr: &str) -> Option<(String, String, String, String)> {
                    let ops = ["<=", ">=", "<>", "=", "<", ">"]; // order matters
                    for op in ops {
                        if let Some(i) = expr.find(op) {
                            let lhs = expr[..i].trim();
                            let rhs = expr[i+op.len()..].trim();
                            if let Some(dot) = lhs.find('.') {
                                let var = lhs[..dot].trim();
                                let prop = lhs[dot+1..].trim();
                                return Some((var.to_string(), prop.to_string(), op.to_string(), rhs.to_string()));
                            }
                        }
                    }
                    None
                }

                fn parse_contains(expr: &str) -> Option<(String, String, String)> {
                    let up = expr.to_uppercase();
                    if let Some(i) = up.find(" CONTAINS ") {
                        let lhs = expr[..i].trim();
                        let rhs = expr[i+10..].trim();
                        if let Some(dot) = lhs.find('.') {
                            let var = lhs[..dot].trim().to_string();
                            let prop = lhs[dot+1..].trim().to_string();
                            return Some((var, prop, rhs.to_string()));
                        }
                    }
                    None
                }

                fn parse_starts_with(expr: &str) -> Option<(String, String, String)> {
                    let up = expr.to_uppercase();
                    if let Some(i) = up.find(" STARTS WITH ") {
                        let lhs = expr[..i].trim();
                        let rhs = expr[i+13..].trim();
                        if let Some(dot) = lhs.find('.') {
                            let var = lhs[..dot].trim().to_string();
                            let prop = lhs[dot+1..].trim().to_string();
                            return Some((var, prop, rhs.to_string()));
                        }
                    }
                    None
                }

                fn parse_ends_with(expr: &str) -> Option<(String, String, String)> {
                    let up = expr.to_uppercase();
                    if let Some(i) = up.find(" ENDS WITH ") {
                        let lhs = expr[..i].trim();
                        let rhs = expr[i+10..].trim();
                        if let Some(dot) = lhs.find('.') {
                            let var = lhs[..dot].trim().to_string();
                            let prop = lhs[dot+1..].trim().to_string();
                            return Some((var, prop, rhs.to_string()));
                        }
                    }
                    None
                }

                let clauses = split_where_and(&w);
                let mut filtered: Vec<HashMap<String, Val>> = Vec::new();
                'rowloop: for row in &rows {
                    // each clause must pass
                    for clause in &clauses {
                        let c = clause.trim();
                        // id compare
                        if let Some((lv, op, rv)) = parse_id_compare(c) {
                            if let (Some(Val::NodeId(a)), Some(Val::NodeId(b))) = (row.get(&lv), row.get(&rv)) {
                                let la = a.as_u128(); let lb = b.as_u128();
                                let pass = match op.as_str() { "<"=>la<lb, "<="=>la<=lb, ">"=>la>lb, ">="=>la>=lb, "="=>la==lb, "<>"=>la!=lb, _=>true };
                                if !pass { continue 'rowloop; }
                            } else { continue 'rowloop; }
                            continue;
                        }
                        // CONTAINS
                        if let Some((var, prop, rhs)) = parse_contains(c) {
                            let val = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                            // Only node props for now
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                if let Some(n) = db.get_node(*id) {
                                    let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                    if !sv.contains(&val) { continue 'rowloop; }
                                } else { continue 'rowloop; }
                            } else { continue 'rowloop; }
                            continue;
                        }
                        // STARTS WITH
                        if let Some((var, prop, rhs)) = parse_starts_with(c) {
                            let val = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                if let Some(n) = db.get_node(*id) {
                                    let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                    if !sv.starts_with(&val) { continue 'rowloop; }
                                } else { continue 'rowloop; }
                            } else { continue 'rowloop; }
                            continue;
                        }
                        // ENDS WITH
                        if let Some((var, prop, rhs)) = parse_ends_with(c) {
                            let val = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                if let Some(n) = db.get_node(*id) {
                                    let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                    if !sv.ends_with(&val) { continue 'rowloop; }
                                } else { continue 'rowloop; }
                            } else { continue 'rowloop; }
                            continue;
                        }
                        // var.prop op literal
                        if let Some((var, prop, op, rhs)) = parse_var_prop_comp(c) {
                            let lit = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                            // Only node props for now
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                if let Some(n) = db.get_node(*id) {
                                    let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                    // numeric compare if both parse
                                    let as_num = |s: &str| s.parse::<f64>().ok();
                                    let pass = if let (Some(a), Some(b)) = (as_num(&sv), as_num(&lit)) {
                                        match op.as_str() { "<"=>a<b, "<="=>a<=b, ">"=>a>b, ">="=>a>=b, "="=> a==b, "<>"=> a!=b, _=>true }
                                    } else {
                                        match op.as_str() { "="=> sv==lit, "<>"=> sv!=lit, "<"=> sv<lit, ">"=> sv>lit, "<="=> sv<=lit, ">="=> sv>=lit, _=> true }
                                    };
                                    if !pass { continue 'rowloop; }
                                } else { continue 'rowloop; }
                            } else { continue 'rowloop; }
                            continue;
                        }
                        // unsupported clause -> fail-safe: do not filter this row out
                    }
                    filtered.push(row.clone());
                }
                rows = filtered;
            }
            Clause::With { items, distinct: _distinct, order_by, skip, limit } => {
                // Project rows to only listed items (variables supported), then apply ORDER BY/SKIP/LIMIT
                // Build sort keys per original rows, then project
                let _single_item = items.len() == 1; // impacts how we interpret pagination
                // Evaluate keys for ordering
                let mut keyed_rows: Vec<(Vec<String>, HashMap<String, Val>)> = Vec::new();
                for r in &rows {
                    // Evaluate sort key vector from order_by
                    let mut key_vals: Vec<String> = Vec::new();
                    if !order_by.is_empty() {
                        for (expr, _asc) in &order_by {
                            match expr {
                                Expr::Var(v) => {
                                    if let Some(Val::NodeId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else if let Some(Val::RelId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else { key_vals.push(String::new()); }
                                }
                                Expr::Prop(inner, prop) => {
                                    if let Expr::Var(v) = &**inner {
                                        if let Some(Val::NodeId(id)) = r.get(v) {
                                            if let Some(n) = db.get_node(*id) { key_vals.push(n.metadata.get(prop).cloned().unwrap_or_default()); }
                                            else { key_vals.push(String::new()); }
                                        } else { key_vals.push(String::new()); }
                                    } else { key_vals.push(String::new()); }
                                }
                                Expr::FuncId(v) => {
                                    if let Some(Val::NodeId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else if let Some(Val::RelId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else { key_vals.push(String::new()); }
                                }
                                Expr::Str(s) => key_vals.push(s.clone()),
                            }
                        }
                    }
                    // Now project variables
                    let mut proj: HashMap<String, Val> = HashMap::new();
                    for it in &items {
                        if let Expr::Var(v) = it {
                            if let Some(val) = r.get(v) { proj.insert(v.clone(), val.clone()); }
                        }
                    }
                    keyed_rows.push((key_vals, proj));
                }
                // Sort if requested
                if !order_by.is_empty() {
                    keyed_rows.sort_by(|a, b| {
                        let ka = &a.0; let kb = &b.0;
                        let mut ord = std::cmp::Ordering::Equal;
                        let len = ka.len().min(kb.len()).min(order_by.len());
                        for i in 0..len {
                            let asc = order_by[i].1;
                            // numeric compare first
                            let (na, nb) = (ka[i].parse::<f64>().ok(), kb[i].parse::<f64>().ok());
                            ord = match (na, nb) {
                                (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
                                _ => ka[i].cmp(&kb[i]),
                            };
                            if !asc { ord = ord.reverse(); }
                            if ord != std::cmp::Ordering::Equal { break; }
                        }
                        ord
                    });
                }
                // Apply SKIP/LIMIT
                let mut start = skip.unwrap_or(0);
                let mut remaining = limit.unwrap_or(usize::MAX);
                let mut new_rows: Vec<HashMap<String, Val>> = Vec::new();
                for (_keys, proj) in keyed_rows.into_iter() {
                    if start > 0 { start -= 1; continue; }
                    if remaining == 0 { break; }
                    new_rows.push(proj);
                    remaining = remaining.saturating_sub(1);
                }
                rows = new_rows;
            }
            Clause::Delete { vars, detach } => {
                use std::collections::HashSet;
                let mut rel_to_del: HashSet<Uuid> = HashSet::new();
                let mut nodes_to_del: HashSet<Uuid> = HashSet::new();
                for row in &rows {
                    for v in &vars {
                        if let Some(val) = row.get(v) {
                            match val {
                                Val::RelId(rid) => { rel_to_del.insert(*rid); }
                                Val::NodeId(nid) => {
                                    if detach {
                                        nodes_to_del.insert(*nid);
                                    } else {
                                        // Only allow delete if node has no relationships
                                        let has_incident = db.relationships.values().any(|r| r.from_node == *nid || r.to_node == *nid);
                                        if has_incident {
                                            return Err(anyhow!("Cannot DELETE node {} with existing relationships; use DETACH DELETE", nid));
                                        }
                                        nodes_to_del.insert(*nid);
                                    }
                                }
                            }
                        }
                    }
                }
                // Delete relationships first
                for rid in rel_to_del { let _ = db.remove_relationship(rid); }
                // Then delete nodes
                for nid in nodes_to_del { let _ = db.remove_node(nid); }
            }
            Clause::Set { items } => {
                // Supported minimal forms:
                // - SET n.prop = <literal>
                // - SET r.prop = <literal>
                // - SET n:NewLabel (node) or r:NewType (relationship)
                // Literals: quoted strings or bare numbers (stored as string)
                for row in &rows {
                    for raw in &items {
                        let s = raw.trim();
                        if s.is_empty() { continue; }
                        // Label change? var:Label
                        if let Some(col) = s.find(':') {
                            let (var, lbl) = s.split_at(col);
                            let var = var.trim();
                            let label = lbl[1..].trim();
                            if label.is_empty() { continue; }
                            if let Some(val) = row.get(var) {
                                match val {
                                    Val::NodeId(nid) => { let _ = db.update_node_label(*nid, label.to_string()); }
                                    Val::RelId(rid) => { let _ = db.update_relationship_label(*rid, label.to_string()); }
                                }
                            }
                            continue;
                        }
                        // Property set: var.prop = value
                        if let Some(eq) = s.find('=') {
                            let (lhs, rhs) = s.split_at(eq);
                            let rhs = rhs[1..].trim();
                            let (var, prop) = if let Some(dot) = lhs.find('.') {
                                (lhs[..dot].trim(), lhs[dot+1..].trim())
                            } else { (lhs.trim(), "") };
                            if prop.is_empty() { continue; }
                            // parse literal value
                            let value = if (rhs.starts_with('"') && rhs.ends_with('"')) || (rhs.starts_with('\'') && rhs.ends_with('\'')) {
                                trim_quotes(rhs)
                            } else {
                                rhs.to_string()
                            };
                            if let Some(val) = row.get(var) {
                                match val {
                                    Val::NodeId(nid) => { let _ = db.upsert_node_metadata(*nid, prop.to_string(), value.clone()); }
                                    Val::RelId(rid) => { let _ = db.upsert_relationship_metadata(*rid, prop.to_string(), value.clone()); }
                                }
                            }
                        }
                    }
                }
            }
            Clause::Remove { items } => {
                // Supported minimal forms:
                // - REMOVE n.prop
                // - REMOVE r.prop
                for row in &rows {
                    for raw in &items {
                        let s = raw.trim();
                        if s.is_empty() { continue; }
                        if let Some(dot) = s.find('.') {
                            let var = s[..dot].trim();
                            let prop = s[dot+1..].trim();
                            if let Some(val) = row.get(var) {
                                match val {
                                    Val::NodeId(nid) => { let _ = db.remove_node_metadata_key(*nid, prop); }
                                    Val::RelId(rid) => { let _ = db.remove_relationship_metadata_key(*rid, prop); }
                                }
                            }
                        }
                    }
                }
            }
            Clause::Create { patterns } => {
                for p in patterns {
                    match p {
                        Pattern::Node(np) => {
                            let mut meta = HashMap::new();
                            for (k, vraw) in &np.props { meta.insert(k.clone(), resolve_param(vraw, params)?); }
                            let label = np.label.unwrap_or_else(|| "_".to_string());
                            let id = db.add_node(label, meta);
                            // bind var if any
                            for row in rows.iter_mut() { if let Some(v) = &np.var { row.insert(v.clone(), Val::NodeId(id)); } }
                        }
                        Pattern::Path { left, rel, right } => {
                            // Require left/right var bound in current rows
                            let mut new_rows = Vec::new();
                            for row in &rows {
                                let from_id = match &left.var { Some(v) => match row.get(v) { Some(Val::NodeId(id)) => *id, _ => continue }, None => continue };
                                let to_id = match &right.var { Some(v) => match row.get(v) { Some(Val::NodeId(id)) => *id, _ => continue }, None => continue };
                                let typ = rel.typ.clone().unwrap_or_else(|| "_".to_string());
                                let mut meta = HashMap::new();
                                for (k, vraw) in &rel.props { meta.insert(k.clone(), resolve_param(vraw, params)?); }
                                if let Some(rid) = db.add_relationship(from_id, to_id, typ.clone(), meta) {
                                    let mut m = row.clone();
                                    if let Some(rv) = &rel.var { m.insert(rv.clone(), Val::RelId(rid)); }
                                    new_rows.push(m);
                                }
                            }
                            if !new_rows.is_empty() { rows = new_rows; }
                        }
                    }
                }
            }
            Clause::Merge { pattern } => {
                // only support relationship merge between bound vars
                if let Pattern::Path { left, rel, right } = pattern {
                    let mut new_rows = Vec::new();
                    for row in &rows {
                        let from_id = match &left.var { Some(v) => match row.get(v) { Some(Val::NodeId(id)) => *id, _ => continue }, None => continue };
                        let to_id = match &right.var { Some(v) => match row.get(v) { Some(Val::NodeId(id)) => *id, _ => continue }, None => continue };
                        let typ = rel.typ.clone().unwrap_or_else(|| "_".to_string());
                        // check exists
                        let mut rid_opt = None;
                        for r in db.relationships.values() {
                            if r.from_node == from_id && r.to_node == to_id && r.label == typ {
                                // if MERGE specified properties, ensure all match
                                let mut all_match = true;
                                for (k, vraw) in &rel.props { let v = resolve_param(vraw, params)?; if r.metadata.get(k) != Some(&v) { all_match = false; break; } }
                                if all_match { rid_opt = Some(r.id); break; }
                            }
                        }
                        let rid = if let Some(rid) = rid_opt { rid } else {
                            let mut meta = HashMap::new();
                            for (k, vraw) in &rel.props { meta.insert(k.clone(), resolve_param(vraw, params)?); }
                            match db.add_relationship(from_id, to_id, typ.clone(), meta) {
                                Some(r) => r,
                                None => {
                                    // If either endpoint is missing (unexpected), skip creating this rel to avoid panic.
                                    continue;
                                }
                            }
                        };
                        let mut m = row.clone();
                        if let Some(rv) = &rel.var { m.insert(rv.clone(), Val::RelId(rid)); }
                        new_rows.push(m);
                    }
                    rows = new_rows;
                } else {
                    return Err(anyhow!("MERGE currently supports only single relationship patterns"));
                }
            }
            Clause::Return { items, distinct, order_by, skip, limit } => {
                // Evaluate per-row projections first into a vector of tuples (keys for sorting, projected rows)
                // Minimal semantics: if multiple items, we still flatten as before but sort only when a single item is returned.
                let single_item = items.len() == 1;
                let mut projected: Vec<(Option<Vec<String>>, Vec<QueryResultRow>)> = Vec::new();
                for r in &rows {
                    let mut out_rows: Vec<QueryResultRow> = Vec::new();
                    for it in &items {
                        match it {
                            Expr::Var(v) => {
                                if let Some(Val::NodeId(id)) = r.get(v) {
                                    if let Some(n) = get_node(db, id) {
                                        out_rows.push(QueryResultRow::Node { id: n.id, label: n.label, metadata: n.metadata });
                                    }
                                } else if let Some(Val::RelId(id)) = r.get(v) {
                                    if let Some(rel) = get_rel(db, id) {
                                        out_rows.push(QueryResultRow::Relationship { id: rel.id, from: rel.from_node, to: rel.to_node, label: rel.label, metadata: rel.metadata });
                                    }
                                }
                            }
                            Expr::Prop(expr, key) => {
                                if let Expr::Var(v) = &**expr {
                                    if let Some(Val::NodeId(id)) = r.get(v) {
                                        if let Some(n) = get_node(db, id) {
                                            if let Some(val) = n.metadata.get(key) {
                                                out_rows.push(QueryResultRow::Info(val.clone()));
                                            }
                                        }
                                    }
                                }
                            }
                            Expr::FuncId(v) => {
                                if let Some(Val::NodeId(id)) = r.get(v) { out_rows.push(QueryResultRow::Info(id.to_string())); }
                                else if let Some(Val::RelId(id)) = r.get(v) { out_rows.push(QueryResultRow::Info(id.to_string())); }
                            }
                            Expr::Str(s) => out_rows.push(QueryResultRow::Info(s.clone())),
                        }
                    }
                    // Build sort keys (as strings) if needed and only for single-item
                    let keys = if single_item && (!order_by.is_empty()) {
                        // evaluate the first order key against the row; support Var/Prop/FuncId
                        let mut key_vals: Vec<String> = Vec::new();
                        for (expr, _asc) in &order_by {
                            match expr {
                                Expr::Var(v) => {
                                    if let Some(Val::NodeId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else if let Some(Val::RelId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else { key_vals.push(String::new()); }
                                }
                                Expr::Prop(inner, prop) => {
                                    if let Expr::Var(v) = &**inner {
                                        if let Some(Val::NodeId(id)) = r.get(v) {
                                            if let Some(n) = get_node(db, id) {
                                                key_vals.push(n.metadata.get(prop).cloned().unwrap_or_default());
                                            } else { key_vals.push(String::new()); }
                                        } else { key_vals.push(String::new()); }
                                    } else { key_vals.push(String::new()); }
                                }
                                Expr::FuncId(v) => {
                                    if let Some(Val::NodeId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else if let Some(Val::RelId(id)) = r.get(v) { key_vals.push(id.to_string()); }
                                    else { key_vals.push(String::new()); }
                                }
                                Expr::Str(s) => key_vals.push(s.clone()),
                            }
                        }
                        Some(key_vals)
                    } else { None };
                    projected.push((keys, out_rows));
                }
                // DISTINCT (single-item only for now): deduplicate by the single projected value
                if distinct && single_item {
                    use std::collections::HashSet;
                    let mut seen: HashSet<String> = HashSet::new();
                    let mut deduped: Vec<(Option<Vec<String>>, Vec<QueryResultRow>)> = Vec::new();
                    for (keys, mut outs) in projected.into_iter() {
                        if outs.is_empty() { continue; }
                        let k = match &outs[0] {
                            QueryResultRow::Node { id, .. } => id.to_string(),
                            QueryResultRow::Relationship { id, .. } => id.to_string(),
                            QueryResultRow::Info(s) => s.clone(),
                        };
                        if seen.insert(k) {
                            deduped.push((keys, vec![outs.remove(0)]));
                        }
                    }
                    projected = deduped;
                }
                // Flatten now or after sorting when applicable
                if single_item && !order_by.is_empty() {
                    projected.sort_by(|a, b| {
                        let ka = a.0.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);
                        let kb = b.0.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);
                        let mut ord = std::cmp::Ordering::Equal;
                        let len = ka.len().min(kb.len()).min(order_by.len());
                        for i in 0..len {
                            let asc = order_by[i].1;
                            // try numeric compare first
                            let (na, nb) = (ka[i].parse::<f64>().ok(), kb[i].parse::<f64>().ok());
                            ord = match (na, nb) {
                                (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
                                _ => ka[i].cmp(&kb[i]),
                            };
                            if !asc { ord = ord.reverse(); }
                            if ord != std::cmp::Ordering::Equal { break; }
                        }
                        ord
                    });
                }
                // Apply SKIP/LIMIT (row-wise; each entry corresponds to one RETURNed row when single item)
                let mut flat: Vec<QueryResultRow> = Vec::new();
                if single_item {
                    let mut start = skip.unwrap_or(0);
                    let mut remaining = limit.unwrap_or(usize::MAX);
                    for (_k, mut rows_for_item) in projected.into_iter() {
                        if rows_for_item.is_empty() { continue; }
                        let r0 = rows_for_item.remove(0);
                        if start > 0 { start -= 1; continue; }
                        if remaining == 0 { break; }
                        flat.push(r0);
                        remaining = remaining.saturating_sub(1);
                    }
                } else {
                    // No ordering or pagination supported in multi-item mode; flatten directly
                    for (_k, rows_for_item) in projected.into_iter() { for rr in rows_for_item { flat.push(rr); } }
                }
                return Ok(flat);
            }
        }
    }

    // default: return all bound nodes as rows
    let mut out = Vec::new();
    for r in rows {
        for (_k, v) in r {
            match v { Val::NodeId(id) => {
                if let Some(n) = db.get_node(id).cloned() { out.push(QueryResultRow::Node { id: n.id, label: n.label, metadata: n.metadata }); }
            }, Val::RelId(id) => {
                if let Some(rel) = db.get_relationship(id).cloned() { out.push(QueryResultRow::Relationship { id: rel.id, from: rel.from_node, to: rel.to_node, label: rel.label, metadata: rel.metadata }); }
            } }
        }
    }
    Ok(out)
}

pub fn execute_cypher(db: &mut GraphDatabase, query: &str) -> Result<Vec<QueryResultRow>> {
    let empty: HashMap<String, String> = HashMap::new();
    execute_cypher_with_params(db, query, &empty)
}
