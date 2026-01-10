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

#[derive(Debug, Clone)]
struct RelPattern {
    var: Option<String>,
    typ: Option<String>,
    // direction: true if ->, false if <-, None for undirected (not supported yet)
    right: bool,
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
    Return { items: Vec<Expr> },
    Create { patterns: Vec<Pattern> },
    Merge { pattern: Pattern },
    Delete { vars: Vec<String>, detach: bool },
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
        let v = body[..col].trim();
        if !v.is_empty() { np.var = Some(v.to_string()); }
        let lab = body[col+1..].trim();
        if !lab.is_empty() { np.label = Some(lab.to_string()); }
    } else {
        np.var = Some(body.to_string());
    }
    if let Some(p) = props { np.props = parse_props(p)?; }
    Ok(np)
}

fn parse_rel_pattern(s: &str) -> Result<RelPattern> {
    // -[r:TYPE]-> or -[:TYPE]-> or -[r]->
    if !s.starts_with("-[") || !s.ends_with("]-") && !s.ends_with("]->") && !s.ends_with("-]") { return Err(anyhow!("invalid rel pattern: {}", s)); }
    let right = s.ends_with("]->");
    let mid = &s[2..s.len()- if right { 3 } else { 2 }];
    let mut var = None; let mut typ = None;
    let mut rest = mid.trim();
    if let Some(col) = rest.find(':') {
        let v = rest[..col].trim();
        if !v.is_empty() { var = Some(v.to_string()); }
        let t = rest[col+1..].trim();
        if !t.is_empty() { typ = Some(t.to_string()); }
    } else if !rest.is_empty() {
        var = Some(rest.to_string());
    }
    Ok(RelPattern { var, typ, right })
}

fn split_top_level_comma(mut s: &str) -> Vec<String> {
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
    let s = s.trim();
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
        let rest_up = rest.to_uppercase();
        let (patterns_str, tail) = if let Some(i) = find_keyword_boundary(&rest_up, "RETURN") {
            (&rest[..i], Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "WHERE") {
            let patterns_str = &rest[..i];
            let w_and_tail = &rest[i+7..];
            // check if there is RETURN after where
            let w_up = w_and_tail.to_uppercase();
            if let Some(k) = find_keyword_boundary(&w_up, "RETURN") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "DELETE") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else if let Some(k) = find_keyword_boundary(&w_up, "DETACH DELETE") {
                where_part = Some(&w_and_tail[..k]);
                (patterns_str, Some(&w_and_tail[k..]))
            } else {
                where_part = Some(w_and_tail);
                (patterns_str, None)
            }
        } else if let Some(i) = find_keyword_boundary(&rest_up, "DELETE") {
            (&rest[..i], Some(&rest[i..]))
        } else if let Some(i) = find_keyword_boundary(&rest_up, "DETACH DELETE") {
            (&rest[..i], Some(&rest[i..]))
        } else { (rest, None) };

        let mut patterns = Vec::new();
        for pat in split_top_level_comma(patterns_str) { if !pat.is_empty() { patterns.push(parse_pattern(&pat)?); } }
        clauses.push(Clause::Match { optional, patterns });
        if let Some(w) = where_part { clauses.push(Clause::Where(w.trim().to_string())); }
        if let Some(t) = tail {
            let t = t.trim();
            let tup = t.to_uppercase();
            if tup.starts_with("RETURN ") {
                let items_str = &t[7..];
                let items = parse_return_items(items_str)?;
                clauses.push(Clause::Return { items });
            } else if tup.starts_with("DELETE ") {
                let vars_str = &t[7..];
                let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
                clauses.push(Clause::Delete { vars, detach: false });
            } else if tup.starts_with("DETACH DELETE ") {
                let vars_str = &t[14..];
                let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
                clauses.push(Clause::Delete { vars, detach: true });
            }
        }
        return Ok(clauses);
    } else if up.starts_with("CREATE ") {
        let body = &q[7..].trim();
        let mut parts = body.splitn(2, " RETURN ");
        let pats = parts.next().unwrap();
        let mut patterns = Vec::new();
        for pat in split_top_level_comma(pats) { if !pat.is_empty() { patterns.push(parse_pattern(&pat)?); } }
        clauses.push(Clause::Create { patterns });
        if let Some(ret) = parts.next() {
            let items = parse_return_items(ret.trim())?;
            clauses.push(Clause::Return { items });
        }
        return Ok(clauses);
    } else if up.starts_with("MERGE ") {
        let body = &q[6..].trim();
        let pattern = parse_pattern(body)?;
        clauses.push(Clause::Merge { pattern });
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

pub fn execute_cypher(db: &mut GraphDatabase, query: &str) -> Result<Vec<QueryResultRow>> {
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
                                    for (k, v) in &np.props { if n.metadata.get(k) != Some(v) { ok = false; break; } }
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
                                // enumerate triples (a)-[r:TYPE]->(b) or undirected (a)-[r]-(b)
                                for (_rid, r) in &db.relationships {
                                    if let Some(t) = &rel.typ { if &r.label != t { continue; } }
                                    let from = db.nodes.get(&r.from_node).unwrap();
                                    let to = db.nodes.get(&r.to_node).unwrap();

                                    // Helper to try match given (L,R) node order
                                    let try_match = |left_np: &NodePattern, right_np: &NodePattern, a: &Node, b: &Node| -> bool {
                                        if let Some(lab) = &left_np.label { if &a.label != lab { return false; } }
                                        for (k, v) in &left_np.props { if a.metadata.get(k) != Some(v) { return false; } }
                                        if let Some(lab) = &right_np.label { if &b.label != lab { return false; } }
                                        for (k, v) in &right_np.props { if b.metadata.get(k) != Some(v) { return false; } }
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
                        partials = new_partials;
                    }
                    next_rows.extend(partials);
                }
                rows = next_rows;
            }
            Clause::Where(_w) => {
                // TODO: implement expression filtering. For now, no-op.
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
            Clause::Create { patterns } => {
                for p in patterns {
                    match p {
                        Pattern::Node(np) => {
                            let mut meta = np.props.clone();
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
                                if let Some(rid) = db.add_relationship(from_id, to_id, typ.clone(), HashMap::new()) {
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
                            if r.from_node == from_id && r.to_node == to_id && r.label == typ { rid_opt = Some(r.id); break; }
                        }
                        let rid = if let Some(rid) = rid_opt { rid } else { db.add_relationship(from_id, to_id, typ.clone(), HashMap::new()).unwrap() };
                        let mut m = row.clone();
                        if let Some(rv) = &rel.var { m.insert(rv.clone(), Val::RelId(rid)); }
                        new_rows.push(m);
                    }
                    rows = new_rows;
                } else {
                    return Err(anyhow!("MERGE currently supports only single relationship patterns"));
                }
            }
            Clause::Return { items } => {
                // Project rows into QueryResultRow list (flatten)
                let mut out: Vec<QueryResultRow> = Vec::new();
                for r in &rows {
                    for it in &items {
                        match it {
                            Expr::Var(v) => {
                                if let Some(Val::NodeId(id)) = r.get(v) {
                                    if let Some(n) = get_node(db, id) {
                                        out.push(QueryResultRow::Node { id: n.id, label: n.label, metadata: n.metadata });
                                    }
                                } else if let Some(Val::RelId(id)) = r.get(v) {
                                    if let Some(rel) = get_rel(db, id) {
                                        out.push(QueryResultRow::Relationship { id: rel.id, from: rel.from_node, to: rel.to_node, label: rel.label, metadata: rel.metadata });
                                    }
                                }
                            }
                            Expr::Prop(expr, key) => {
                                if let Expr::Var(v) = &**expr {
                                    if let Some(Val::NodeId(id)) = r.get(v) {
                                        if let Some(n) = get_node(db, id) {
                                            if let Some(val) = n.metadata.get(key) {
                                                out.push(QueryResultRow::Info(val.clone()));
                                            }
                                        }
                                    }
                                }
                            }
                            Expr::FuncId(v) => {
                                if let Some(Val::NodeId(id)) = r.get(v) { out.push(QueryResultRow::Info(id.to_string())); }
                                else if let Some(Val::RelId(id)) = r.get(v) { out.push(QueryResultRow::Info(id.to_string())); }
                            }
                            Expr::Str(s) => out.push(QueryResultRow::Info(s.clone())),
                        }
                    }
                }
                return Ok(out);
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
