use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use time::{macros::format_description, OffsetDateTime};
use uuid::Uuid;

use crate::graph_utils::graph::{GraphDatabase, NodeId};
use super::cypher_spec::{execute_cypher, execute_cypher_with_params};

#[derive(Debug, Clone)]
pub enum QueryResultRow {
    Node { id: NodeId, label: String, metadata: HashMap<String, String> },
    Relationship { id: Uuid, from: NodeId, to: NodeId, label: String, metadata: HashMap<String, String> },
    #[allow(dead_code)]
    Info(String),
}

#[derive(Debug, Default, Clone)]
pub struct QueryOutcome {
    pub rows: Vec<QueryResultRow>,
    pub affected_nodes: usize,
    pub affected_relationships: usize,
    pub mutated: bool,
}

fn log_path_for_now() -> PathBuf {
    let base = PathBuf::from("assets/logs");
    let now = OffsetDateTime::now_utc();
    let fmt = format_description!("[year][month][day]");
    let date = now.format(&fmt).unwrap_or_else(|_| "unknown".into());
    base.join(format!("queries_{}.log", date))
}

fn log_query(query: &str, outcome: &Result<QueryOutcome>) {
    let _ = create_dir_all("assets/logs");
    let mut path = log_path_for_now();
    // ensure parent exists
    if let Some(parent) = path.parent() { let _ = create_dir_all(parent); }
    let now = OffsetDateTime::now_utc();
    let ts_fmt = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    let ts = now.format(&ts_fmt).unwrap_or_else(|_| "".into());
    let status = match outcome {
        Ok(o) => format!("OK mutated={} nodes={} rels={}", o.mutated, o.affected_nodes, o.affected_relationships),
        Err(e) => format!("ERR {}", e),
    };
    let line = format!("{} | {}\n{}\n\n", ts, status, query.trim());
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&mut path) {
        let _ = file.write_all(line.as_bytes());
    }
}

fn _split_statements(input: &str) -> Vec<String> {
    // Primary split by ';'. Additionally, split when a new line starts with a Cypher keyword (CREATE/MATCH/OPTIONAL MATCH/MERGE/RETURN/DELETE/DETACH DELETE)
    // This allows multi-line separate statements without semicolons, while preserving multi-line bodies like CREATE with patterns on following lines.
    let mut parts: Vec<String> = Vec::new();
    for chunk in input.split(';') {
        let mut acc = String::new();
        for line in chunk.lines() {
            let trimmed = line.trim_start();
            let up = trimmed.to_uppercase();
            let is_keyword_line = up.starts_with("CREATE") || up.starts_with("MATCH ") || up.starts_with("OPTIONAL MATCH ") || up.starts_with("MERGE ") || up.starts_with("RETURN ") || up.starts_with("DETACH DELETE ") || up.starts_with("DELETE ");
            if is_keyword_line && !acc.trim().is_empty() {
                // start a new statement
                parts.push(acc.trim().to_string());
                acc = String::new();
            }
            if !acc.is_empty() { acc.push('\n'); }
            acc.push_str(line);
        }
        if !acc.trim().is_empty() { parts.push(acc.trim().to_string()); }
    }
    parts.into_iter().filter(|s| !s.trim().is_empty()).collect()
}

pub fn execute_query(db: &mut GraphDatabase, query: &str) -> Result<QueryOutcome> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty query"));
    }

    // We allow multiple statements separated by semicolons; execute sequentially
    let mut outcome = QueryOutcome::default();
    let mut any_mut = false;
    for stmt in trimmed.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() { continue; }
        let upper = stmt.to_uppercase();
        // First: legacy minimal Cypher-style handler for pairwise MATCH...MERGE in one statement
        let res = if upper.starts_with("MATCH (") && upper.contains(" MERGE ") {
            // Legacy minimal Cypher-style pairwise support (kept for compatibility)
            exec_cypher_match_merge(db, stmt)
        // If the statement appears to be OpenCypher, route to the Cypher engine.
        // Detect by keywords and forms that are NOT the legacy custom commands.
        } else if (upper.starts_with("MATCH ") && stmt[6..].trim_start().starts_with('(')) ||
        // OPTIONAL MATCH with '(' only
        (upper.starts_with("OPTIONAL MATCH ") && stmt[15..].trim_start().starts_with('(')) ||
        // MERGE is Cypher-only
        upper.starts_with("MERGE ") ||
        // RETURN is Cypher-only
        upper.starts_with("RETURN ") ||
        // SET / REMOVE are Cypher-only
        upper.starts_with("SET ") || upper.starts_with("REMOVE ") ||
        // DELETE / DETACH DELETE are Cypher-only, but avoid legacy DELETE NODE/REL
        (upper.starts_with("DELETE ") && !upper.starts_with("DELETE NODE ") && !upper.starts_with("DELETE REL ")) ||
        upper.starts_with("DETACH DELETE ") ||
        // CREATE with '(' pattern (avoid legacy CREATE NODE/REL)
        (upper.starts_with("CREATE") && stmt[6..].trim_start().starts_with('(')) {
            let rows = execute_cypher(db, stmt)?;
            // conservatively mark mutated if statement starts with CREATE or MERGE
            let mutated = upper.starts_with("CREATE")
                || upper.starts_with("MERGE ")
                || upper.starts_with("SET ")
                || upper.starts_with("REMOVE ")
                || (upper.starts_with("DELETE ") && !upper.starts_with("DELETE NODE ") && !upper.starts_with("DELETE REL "))
                || upper.starts_with("DETACH DELETE ");
            Ok((rows, 0, 0, mutated))
        } else if upper.starts_with("CREATE NODE ") {
            exec_create_node(db, &stmt[12..])
        } else if upper.starts_with("CREATE REL ") {
            exec_create_rel(db, &stmt[11..])
        } else if upper.starts_with("MATCH NODE ") {
            exec_match_node(db, &stmt[11..])
        } else if upper.starts_with("MATCH REL ") {
            exec_match_rel(db, &stmt[10..])
        } else if upper.starts_with("DELETE NODE ") {
            exec_delete_node(db, &stmt[12..]).map(|cnt| (Vec::new(), cnt, 0, true))
        } else if upper.starts_with("DELETE REL ") {
            exec_delete_rel(db, &stmt[11..]).map(|cnt| (Vec::new(), 0, cnt, true))
        } else {
            return Err(anyhow!("unrecognized statement: {}", stmt));
        }?;

        let (rows, n_cnt, r_cnt, mutated) = res;
        outcome.rows.extend(rows);
        outcome.affected_nodes += n_cnt;
        outcome.affected_relationships += r_cnt;
        any_mut = any_mut || mutated;
    }
    outcome.mutated = any_mut;
    Ok(outcome)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn execute_and_log(db: &mut GraphDatabase, query: &str) -> Result<QueryOutcome> {
    let res = execute_query(db, query);
    log_query(query, &res);
    res
}

/// Execute a query with parameters (for OpenCypher `$param` usage).
#[cfg_attr(not(test), allow(dead_code))]
pub fn execute_query_with_params(
    db: &mut GraphDatabase,
    query: &str,
    params: &HashMap<String, String>,
)
-> Result<QueryOutcome> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty query"));
    }

    let mut outcome = QueryOutcome::default();
    let mut any_mut = false;
    for stmt in trimmed.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() { continue; }
        let upper = stmt.to_uppercase();
        // First: legacy minimal Cypher-style handler for pairwise MATCH...MERGE
        let res = if upper.starts_with("MATCH (") && upper.contains(" MERGE ") {
            exec_cypher_match_merge(db, stmt)
        // True Cypher engine path
        } else if (upper.starts_with("MATCH ") && stmt[6..].trim_start().starts_with('(')) ||
        (upper.starts_with("OPTIONAL MATCH ") && stmt[15..].trim_start().starts_with('(')) ||
        upper.starts_with("MERGE ") ||
        upper.starts_with("RETURN ") ||
        (upper.starts_with("DELETE ") && !upper.starts_with("DELETE NODE ") && !upper.starts_with("DELETE REL ")) ||
        upper.starts_with("DETACH DELETE ") ||
        (upper.starts_with("CREATE ") && stmt[7..].trim_start().starts_with('(')) {
            let rows = execute_cypher_with_params(db, stmt, params)?;
            let mutated = upper.starts_with("CREATE ") || upper.starts_with("MERGE ") || (upper.starts_with("DELETE ") && !upper.starts_with("DELETE NODE ") && !upper.starts_with("DELETE REL ")) || upper.starts_with("DETACH DELETE ");
            Ok((rows, 0, 0, mutated))
        } else if upper.starts_with("CREATE NODE ") {
            exec_create_node(db, &stmt[12..])
        } else if upper.starts_with("CREATE REL ") {
            exec_create_rel(db, &stmt[11..])
        } else if upper.starts_with("MATCH NODE ") {
            exec_match_node(db, &stmt[11..])
        } else if upper.starts_with("MATCH REL ") {
            exec_match_rel(db, &stmt[10..])
        } else if upper.starts_with("DELETE NODE ") {
            exec_delete_node(db, &stmt[12..]).map(|cnt| (Vec::new(), cnt, 0, true))
        } else if upper.starts_with("DELETE REL ") {
            exec_delete_rel(db, &stmt[11..]).map(|cnt| (Vec::new(), 0, cnt, true))
        } else {
            return Err(anyhow!("unrecognized statement: {}", stmt));
        }?;

        let (rows, n_cnt, r_cnt, mutated) = res;
        outcome.rows.extend(rows);
        outcome.affected_nodes += n_cnt;
        outcome.affected_relationships += r_cnt;
        any_mut = any_mut || mutated;
    }
    outcome.mutated = any_mut;
    Ok(outcome)
}

/// Same as execute_and_log but accepts parameters for OpenCypher `$param`s.
#[cfg_attr(not(test), allow(dead_code))]
pub fn _execute_and_log_with_params(
    db: &mut GraphDatabase,
    query: &str,
    params: &HashMap<String, String>,
) -> Result<QueryOutcome> {
    let res = execute_query_with_params(db, query, params);
    log_query(query, &res);
    res
}

// Split on a top-level WHERE (case-insensitive). Returns (head, where_clause)
fn split_where(rest: &str) -> (String, Option<String>) {
    // naive approach: find " WHERE " (case-insensitive). Also support trailing where without spaces around
    let upper = rest.to_uppercase();
    if let Some(idx) = upper.find(" WHERE ") {
        let head = rest[..idx].trim().to_string();
        let tail = rest[idx + 7..].trim().to_string();
        (head, if tail.is_empty() { None } else { Some(tail) })
    } else if let Some(idx) = upper.find(" WHERE") {
        let head = rest[..idx].trim().to_string();
        let tail = rest[idx + 6..].trim().to_string();
        (head, if tail.is_empty() { None } else { Some(tail) })
    } else {
        (rest.trim().to_string(), None)
    }
}

fn parse_label_and_props(rest: &str) -> Result<(String, HashMap<String, String>)> {
    // Expect: Label {k:"v", a:"b"} or just Label
    let mut label = rest.trim().to_string();
    let mut props: HashMap<String, String> = HashMap::new();
    if let Some(idx) = rest.find('{') {
        label = rest[..idx].trim().to_string();
        let after = &rest[idx..];
        if let Some(end_idx) = after.rfind('}') {
            let inside = &after[1..end_idx];
            props = parse_keyvals(inside)?;
        }
    }
    if label.is_empty() { return Err(anyhow!("missing label")); }
    Ok((label, props))
}

#[derive(Debug, Clone)]
enum WhereCond {
    // Nodes and Relationships
    IdEquals(Uuid),
    LabelEquals(String),
    HasKey(String),
    MetaEq(String, String),
    MetaNe(String, String),
    // Relationships only
    FromEquals(Uuid),
    ToEquals(Uuid),
}

fn parse_where_conds(s: &str) -> Result<Vec<WhereCond>> {
    // Conditions are separated by AND (case-insensitive)
    let mut out = Vec::new();
    // allow multi-line safety (we treat newlines/semicolons as plain text within this WHERE, since the parser splits statements earlier)
    // Better: manually scan tokens separated by 'AND'
    let mut start = 0usize;
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut conds: Vec<&str> = Vec::new();
    while i < bytes.len() {
        // try to match 'AND' case-insensitive with word boundaries
        if i + 3 <= bytes.len() {
            let sub = &s[i..i+3];
            if sub.eq_ignore_ascii_case("AND") {
                // word boundary: previous and next must be whitespace or punctuation
                let prev_ok = i == 0 || s[..i].chars().last().map(|ch| ch.is_whitespace() || ch == ')' ).unwrap_or(true);
                let next_ok = i + 3 >= s.len() || s[i+3..].chars().next().map(|ch| ch.is_whitespace() || ch == '(' ).unwrap_or(true);
                if prev_ok && next_ok {
                    conds.push(s[start..i].trim());
                    i += 3;
                    start = i;
                    continue;
                }
            }
        }
        i += 1;
    }
    conds.push(s[start..].trim());

    for c in conds.into_iter().filter(|c| !c.is_empty()) {
        let cu = c.to_uppercase();
        if cu.starts_with("HAS(") && c.ends_with(')') {
            let inside = &c[4..c.len()-1];
            let key = inside.trim().trim_matches('"').trim_matches('\'');
            if key.is_empty() { return Err(anyhow!("WHERE has() requires a key")); }
            out.push(WhereCond::HasKey(key.to_string()));
            continue;
        }
        // inequality key!="v"
        if let Some(pos) = c.find("!=") {
            let key = c[..pos].trim();
            let val = c[pos+2..].trim().trim_matches('"').trim_matches('\'');
            if key.eq_ignore_ascii_case("id") || key.eq_ignore_ascii_case("label")
                || key.eq_ignore_ascii_case("from") || key.eq_ignore_ascii_case("to") {
                return Err(anyhow!("'!=' supported only for metadata keys"));
            }
            if key.is_empty() { return Err(anyhow!("missing key before !=")); }
            out.push(WhereCond::MetaNe(key.to_string(), val.to_string()));
            continue;
        }
        // equality key="v" or id=uuid or label=Label or from/to=uuid
        if let Some(pos) = c.find('=') {
            let key = c[..pos].trim();
            let val_raw = c[pos+1..].trim();
            if key.eq_ignore_ascii_case("id") {
                let id = Uuid::parse_str(val_raw.trim_matches('"'))?;
                out.push(WhereCond::IdEquals(id));
                continue;
            }
            if key.eq_ignore_ascii_case("from") {
                let id = Uuid::parse_str(val_raw.trim_matches('"'))?;
                out.push(WhereCond::FromEquals(id));
                continue;
            }
            if key.eq_ignore_ascii_case("to") {
                let id = Uuid::parse_str(val_raw.trim_matches('"'))?;
                out.push(WhereCond::ToEquals(id));
                continue;
            }
            if key.eq_ignore_ascii_case("label") {
                let v = val_raw.trim_matches('"').trim_matches('\'').to_string();
                out.push(WhereCond::LabelEquals(v));
                continue;
            }
            // metadata equality requires quoted value but we'll accept bare too
            let v = val_raw.trim_matches('"').trim_matches('\'').to_string();
            if key.is_empty() { return Err(anyhow!("missing key before =")); }
            out.push(WhereCond::MetaEq(key.to_string(), v));
            continue;
        }
        return Err(anyhow!("unrecognized WHERE condition: {}", c));
    }
    Ok(out)
}

fn parse_keyvals(s: &str) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for part in s.split(',') {
        let p = part.trim();
        if p.is_empty() { continue; }
        let mut kv = p.splitn(2, ':');
        let k = kv.next().ok_or_else(|| anyhow!("missing key"))?.trim();
        let v = kv.next().ok_or_else(|| anyhow!("missing value for {}", k))?.trim();
        let v = v.trim_matches('"').trim_matches('\'');
        map.insert(k.to_string(), v.to_string());
    }
    Ok(map)
}

// Minimal openCypher-style support for pattern-based pair matching and merge
// Supports statements like:
//   MATCH (a:Label), (b:Label) [WHERE id(a) < id(b) | id(a) <> id(b)] MERGE (a)-[:TYPE]->(b)
// Limitations: single label per variable; WHERE only supports id(var) comparisons using <,>,<=,>=,=,<>
fn exec_cypher_match_merge(db: &mut GraphDatabase, stmt: &str) -> Result<(Vec<QueryResultRow>, usize, usize, bool)> {
    // Split into MATCH ... [WHERE ...] MERGE ...
    let up = stmt.to_uppercase();
    let match_pos = up.find("MATCH ").ok_or_else(|| anyhow!("invalid MATCH/MERGE statement"))?;
    let merge_pos = up.rfind(" MERGE ").ok_or_else(|| anyhow!("MATCH ... MERGE ... required"))?;
    if merge_pos <= match_pos { return Err(anyhow!("MERGE must come after MATCH")); }
    let match_part = stmt[match_pos + 6..merge_pos].trim();
    let merge_part = stmt[merge_pos + 7..].trim();

    // Extract optional WHERE from match_part
    let (patterns_part, where_opt) = split_where(match_part);
    // Expect two node patterns separated by comma: (a:Label), (b:Label)
    let mut pats = patterns_part.split(',').map(|s| s.trim());
    let p1 = pats.next().ok_or_else(|| anyhow!("missing first pattern"))?;
    let p2 = pats.next().ok_or_else(|| anyhow!("missing second pattern"))?;
    if pats.next().is_some() { return Err(anyhow!("only two node patterns are supported")); }

    fn parse_var_label(p: &str) -> Result<(String, String)> {
        // form: (var:Label) or (var)
        let p = p.trim();
        if !p.starts_with('(') || !p.ends_with(')') { return Err(anyhow!("invalid node pattern: {}", p)); }
        let inside = &p[1..p.len()-1];
        let (var, label) = if let Some(col) = inside.find(':') {
            (inside[..col].trim().to_string(), inside[col+1..].trim().to_string())
        } else {
            (inside.trim().to_string(), String::new())
        };
        if var.is_empty() { return Err(anyhow!("variable name required in node pattern")); }
        Ok((var, label))
    }

    let (var_a, label_a) = parse_var_label(p1)?;
    let (var_b, label_b) = parse_var_label(p2)?;
    // For now require labels on both and allow same label or different
    if label_a.is_empty() || label_b.is_empty() { return Err(anyhow!("labels required in MATCH node patterns")); }

    // Collect candidate node sets by label
    let ids_a = db.find_node_ids_by_label(&label_a);
    let ids_b = db.find_node_ids_by_label(&label_b);

    // WHERE: only id(var) comparator id(var)
    enum CmpOp { Lt, Lte, Gt, Gte, Eq, Ne }
    let mut cmp_filter: Option<(CmpOp, String, String)> = None; // (op, leftVar, rightVar)
    if let Some(w) = where_opt {
        // Normalize spaces and case a bit; expect pattern like: id(a) < id(b)
        let wu = w.replace(" ", "");
        // Identify operator by precedence
        let (op, sym) = if let Some(_i) = wu.find("<=") { (CmpOp::Lte, "<=") }
            else if let Some(_i) = wu.find(">=") { (CmpOp::Gte, ">=") }
            else if let Some(_i) = wu.find("<>") { (CmpOp::Ne, "<>") }
            else if let Some(_i) = wu.find('<') { (CmpOp::Lt, "<") }
            else if let Some(_i) = wu.find('>') { (CmpOp::Gt, ">") }
            else if let Some(_i) = wu.find('=') { (CmpOp::Eq, "=") }
            else { return Err(anyhow!("unsupported WHERE comparator; use <,>,<=,>=,=,<>")); };
        let parts: Vec<&str> = wu.split(sym).collect();
        if parts.len() != 2 { return Err(anyhow!("malformed WHERE clause")); }
        let parse_id_fn = |s: &str| -> Result<String> {
            if !s.to_uppercase().starts_with("ID(") || !s.ends_with(')') { return Err(anyhow!("WHERE must use id(var)")); }
            let v = s[3..s.len()-1].to_string();
            if v.is_empty() { return Err(anyhow!("empty variable in id()")); }
            Ok(v)
        };
        let left = parse_id_fn(parts[0])?;
        let right = parse_id_fn(parts[1])?;
        cmp_filter = Some((op, left, right));
    }

    // Helper to compare UUID order
    let cmp = |a: &Uuid, b: &Uuid, op: &CmpOp| -> bool {
        let au = a.as_u128();
        let bu = b.as_u128();
        match op {
            CmpOp::Lt => au < bu,
            CmpOp::Lte => au <= bu,
            CmpOp::Gt => au > bu,
            CmpOp::Gte => au >= bu,
            CmpOp::Eq => au == bu,
            CmpOp::Ne => au != bu,
        }
    };

    // Parse MERGE pattern: (varA)-[:TYPE]->(varB)
    let mp = merge_part.trim();
    // very minimal parse
    let m_up = mp.to_uppercase();
    if !mp.starts_with('(') || !m_up.contains(")-[:") || !m_up.contains("]->(") || !mp.ends_with(')') {
        return Err(anyhow!("unsupported MERGE pattern; expected (a)-[:TYPE]->(b)"));
    }
    // Extract left var
    let left_end = mp.find(')').ok_or_else(|| anyhow!("bad MERGE left"))?;
    let left_var = mp[1..left_end].trim().to_string();
    // Extract type
    let type_start = mp[left_end..].find("[:").ok_or_else(|| anyhow!("missing [:TYPE]"))? + left_end + 2;
    let type_end = mp[type_start..].find(']').ok_or_else(|| anyhow!("missing ] in MERGE type"))? + type_start;
    let rel_type = mp[type_start..type_end].trim().to_string();
    // Extract right var after "]->("
    let arrow = mp[type_end..].find("->(").ok_or_else(|| anyhow!("missing ->( in MERGE"))? + type_end;
    let right_start = arrow + 3;
    if !mp.ends_with(')') { return Err(anyhow!("missing closing ) for MERGE right var")); }
    let right_var = mp[right_start..mp.len()-1].trim().to_string();

    // Sanity check variables map
    let map_var = |name: &str| -> Result<&str> {
        if name == var_a { Ok("A") } else if name == var_b { Ok("B") } else { Err(anyhow!("MERGE references unknown variable: {}", name)) }
    };
    map_var(&left_var)?; map_var(&right_var)?; // validate

    // Build all pairs according to WHERE comparator if present
    let mut rows = Vec::new();
    let mut rel_count = 0usize;
    let mut created = false;

    // Precompute existing relationships set for MERGE semantics: (from,to,label)
    let mut exists = std::collections::HashSet::<(Uuid, Uuid, String)>::new();
    for r in db.relationships.values() {
        exists.insert((r.from_node, r.to_node, r.label.clone()));
    }

    for a_id in &ids_a {
        for b_id in &ids_b {
            // variable self equality allowed only if var names differ; but if it's the same label and same set, allow a!=b unless WHERE explicitly allows equals
            if var_a == var_b && a_id == b_id { continue; }
            if let Some((op, l, r)) = &cmp_filter {
                let (lv, rv) = if &l[..] == var_a && &r[..] == var_b {
                    (a_id, b_id)
                } else if &l[..] == var_b && &r[..] == var_a {
                    (b_id, a_id)
                } else {
                    // comparator references unknown variables
                    return Err(anyhow!("WHERE references unknown variables"));
                };
                if !cmp(lv, rv, op) { continue; }
            }
            // Determine from/to based on MERGE order (left_var -> right_var)
            let (from, to) = if left_var == var_a && right_var == var_b {
                (*a_id, *b_id)
            } else if left_var == var_b && right_var == var_a {
                (*b_id, *a_id)
            } else {
                return Err(anyhow!("MERGE variable order does not match MATCH variables"));
            };
            let key = (from, to, rel_type.clone());
            if !exists.contains(&key) {
                if let Some(rid) = db.add_relationship(from, to, rel_type.clone(), HashMap::new()) {
                    exists.insert(key);
                    rel_count += 1;
                    created = true;
                    if let Some(r) = db.get_relationship(rid).cloned() {
                        rows.push(QueryResultRow::Relationship { id: r.id, from: r.from_node, to: r.to_node, label: r.label, metadata: r.metadata });
                    }
                }
            }
        }
    }

    Ok((rows, 0, rel_count, created))
}

fn exec_create_node(db: &mut GraphDatabase, rest: &str) -> Result<(Vec<QueryResultRow>, usize, usize, bool)> {
    // rest: Label {k:"v", ...}
    let (label, props) = parse_label_and_props(rest)?;
    let id = db.add_node(label.clone(), props.clone());
    let mut rows = Vec::new();
    if let Some(n) = db.get_node(id).cloned() {
        rows.push(QueryResultRow::Node { id: n.id, label: n.label, metadata: n.metadata });
    }
    Ok((rows, 1, 0, true))
}

fn exec_create_rel(db: &mut GraphDatabase, rest: &str) -> Result<(Vec<QueryResultRow>, usize, usize, bool)> {
    // rest: from=<uuid> to=<uuid> label=Label {k:"v", ...}
    let mut from: Option<Uuid> = None;
    let mut to: Option<Uuid> = None;
    let mut label: Option<String> = None;
    let mut props: HashMap<String, String> = HashMap::new();

    // Split on spaces but keep brace content for props
    let mut cursor = rest.trim();
    // properties block
    if let Some(brace_idx) = cursor.find('{') {
        if let Some(end) = cursor.rfind('}') { props = parse_keyvals(&cursor[brace_idx+1..end])?; cursor = &cursor[..brace_idx]; }
    }
    for token in cursor.split_whitespace() {
        let up = token.to_uppercase();
        if up.starts_with("FROM=") { from = Some(Uuid::parse_str(&token[5..])?); }
        else if up.starts_with("TO=") { to = Some(Uuid::parse_str(&token[3..])?); }
        else if up.starts_with("LABEL=") { label = Some(token[6..].to_string()); }
    }
    let from = from.ok_or_else(|| anyhow!("missing from uuid"))?;
    let to = to.ok_or_else(|| anyhow!("missing to uuid"))?;
    let label = label.ok_or_else(|| anyhow!("missing label"))?;
    let id = db.add_relationship(from, to, label.clone(), props.clone())
        .ok_or_else(|| anyhow!("invalid endpoint(s) for relationship"))?;
    let mut rows = Vec::new();
    if let Some(r) = db.get_relationship(id).cloned() {
        rows.push(QueryResultRow::Relationship { id: r.id, from: r.from_node, to: r.to_node, label: r.label, metadata: r.metadata });
    }
    Ok((rows, 0, 1, true))
}

fn exec_match_node(db: &GraphDatabase, rest: &str) -> Result<(Vec<QueryResultRow>, usize, usize, bool)> {
    // Support optional WHERE after the label/props
    let (head, where_clause) = split_where(rest);
    let (label, props) = parse_label_and_props(&head)?;
    let mut ids = db.find_node_ids_by_label(&label);
    // Filter by props
    if !props.is_empty() {
        ids.retain(|id| {
            db.get_node(*id).map(|n| props.iter().all(|(k, v)| n.metadata.get(k).map(|m| m == v).unwrap_or(false))).unwrap_or(false)
        });
    }
    // Apply WHERE conditions, if any
    let conds = if let Some(ws) = where_clause { parse_where_conds(&ws)? } else { Vec::new() };
    if !conds.is_empty() {
        ids.retain(|id| {
            if let Some(n) = db.get_node(*id) {
                for c in &conds {
                    match c {
                        WhereCond::IdEquals(u) => { if &n.id != u { return false; } }
                        WhereCond::LabelEquals(l) => { if &n.label != l { return false; } }
                        WhereCond::HasKey(k) => { if !n.metadata.contains_key(k) { return false; } }
                        WhereCond::MetaEq(k, v) => { if n.metadata.get(k).map(|m| m == v).unwrap_or(false) == false { return false; } }
                        WhereCond::MetaNe(k, v) => { if n.metadata.get(k).map(|m| m == v).unwrap_or(false) { return false; } }
                        // Relationship-only filters are ignored for nodes
                        WhereCond::FromEquals(_) | WhereCond::ToEquals(_) => { return false; }
                    }
                }
                true
            } else { false }
        });
    }
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(n) = db.get_node(id).cloned() {
            rows.push(QueryResultRow::Node { id: n.id, label: n.label, metadata: n.metadata });
        }
    }
    Ok((rows, 0, 0, false))
}

fn exec_match_rel(db: &GraphDatabase, rest: &str) -> Result<(Vec<QueryResultRow>, usize, usize, bool)> {
    // Support optional WHERE after the label/props
    let (head, where_clause) = split_where(rest);
    let (label, props) = parse_label_and_props(&head)?;
    let mut ids = db.find_relationship_ids_by_label(&label);
    if !props.is_empty() {
        ids.retain(|rid| {
            db.get_relationship(*rid).map(|r| props.iter().all(|(k, v)| r.metadata.get(k).map(|m| m == v).unwrap_or(false))).unwrap_or(false)
        });
    }
    let conds = if let Some(ws) = where_clause { parse_where_conds(&ws)? } else { Vec::new() };
    if !conds.is_empty() {
        ids.retain(|rid| {
            if let Some(r) = db.get_relationship(*rid) {
                for c in &conds {
                    match c {
                        WhereCond::IdEquals(u) => { if &r.id != u { return false; } }
                        WhereCond::LabelEquals(l) => { if &r.label != l { return false; } }
                        WhereCond::HasKey(k) => { if !r.metadata.contains_key(k) { return false; } }
                        WhereCond::MetaEq(k, v) => { if r.metadata.get(k).map(|m| m == v).unwrap_or(false) == false { return false; } }
                        WhereCond::MetaNe(k, v) => { if r.metadata.get(k).map(|m| m == v).unwrap_or(false) { return false; } }
                        WhereCond::FromEquals(u) => { if &r.from_node != u { return false; } }
                        WhereCond::ToEquals(u) => { if &r.to_node != u { return false; } }
                    }
                }
                true
            } else { false }
        });
    }
    let mut rows = Vec::with_capacity(ids.len());
    for rid in ids {
        if let Some(r) = db.get_relationship(rid).cloned() {
            rows.push(QueryResultRow::Relationship { id: r.id, from: r.from_node, to: r.to_node, label: r.label, metadata: r.metadata });
        }
    }
    Ok((rows, 0, 0, false))
}

fn exec_delete_node(db: &mut GraphDatabase, rest: &str) -> Result<usize> {
    let id = parse_uuid_from(rest)?;
    let removed = db.remove_node(id);
    Ok(if removed { 1 } else { 0 })
}

fn exec_delete_rel(db: &mut GraphDatabase, rest: &str) -> Result<usize> {
    let id = parse_uuid_from(rest)?;
    let removed = db.remove_relationship(id);
    Ok(if removed { 1 } else { 0 })
}

fn parse_uuid_from(s: &str) -> Result<Uuid> { Uuid::parse_str(s.trim()).map_err(|e| anyhow!("invalid uuid: {}", e)) }
