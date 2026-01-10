use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use time::{macros::format_description, OffsetDateTime};
use uuid::Uuid;

use crate::graph_utils::graph::{GraphDatabase, NodeId};

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
        let res = if upper.starts_with("CREATE NODE ") {
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

pub fn execute_and_log(db: &mut GraphDatabase, query: &str) -> Result<QueryOutcome> {
    let res = execute_query(db, query);
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
