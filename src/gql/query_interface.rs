use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use time::{macros::format_description, OffsetDateTime};
use uuid::Uuid;
use rayon::prelude::*;

use crate::graph_utils::graph::{GraphDatabase, NodeId};
use crate::semantic::embeddings::NodeEmbeddingIndex;

/// Global embedding cache for reusing computed embeddings across queries
#[allow(dead_code)]
static EMBEDDING_CACHE: once_cell::sync::Lazy<Mutex<Option<(NodeEmbeddingIndex, usize)>>> = 
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Set the global embedding cache
#[allow(dead_code)]
pub fn set_embedding_cache(index: NodeEmbeddingIndex, node_count: usize) {
    if let Ok(mut cache) = EMBEDDING_CACHE.lock() {
        *cache = Some((index, node_count));
    }
}

/// Get a reference to the embedding cache (returns None if not initialized)
#[allow(dead_code)]
pub fn get_embedding_cache() -> Option<(NodeEmbeddingIndex, usize)> {
    if let Ok(cache) = EMBEDDING_CACHE.lock() {
        cache.clone()
    } else {
        None
    }
}

/// Clear the embedding cache
#[allow(dead_code)]
pub fn clear_embedding_cache() {
    if let Ok(mut cache) = EMBEDDING_CACHE.lock() {
        *cache = None;
    }
}
use crate::graph_utils::algorithms::{pagerank, betweenness_centrality, shortest_path, astar_path, all_paths};
use crate::graph_utils::temporal::{get_timeline, get_timestamp_range, nodes_in_range, graph_at_time};
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
    // Primary split by ';'. We avoid splitting if the semicolon is inside quotes or braces.
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_quote = None;
    let mut brace_depth: i32 = 0;
    
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '"' | '\'' => {
                if in_quote == Some(c) {
                    in_quote = None;
                } else if in_quote.is_none() {
                    in_quote = Some(c);
                }
                current.push(c);
            }
            '{' | '(' | '[' if in_quote.is_none() => {
                brace_depth += 1;
                current.push(c);
            }
            '}' | ')' | ']' if in_quote.is_none() => {
                brace_depth = brace_depth.saturating_sub(1);
                current.push(c);
            }
            ';' if in_quote.is_none() && brace_depth == 0 => {
                let s = current.trim().to_string();
                if !s.is_empty() {
                    statements.push(s);
                }
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
        i += 1;
    }
    let s = current.trim().to_string();
    if !s.is_empty() {
        statements.push(s);
    }
    statements
}

/// Batch consecutive CREATE statements into single combined statements.
/// This allows variable bindings from one CREATE to be available in subsequent CREATEs.
/// e.g., "CREATE (p1:Person)" followed by "CREATE (p1)-[:REL]->(c1)" becomes
/// "CREATE (p1:Person) CREATE (p1)-[:REL]->(c1)" so p1 is bound when the relationship is created.
fn batch_consecutive_creates(statements: &[String]) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    let mut create_batch: Vec<String> = Vec::new();
    
    for stmt in statements {
        let trimmed = stmt.trim();
        let upper = trimmed.to_uppercase();
        
        // Check if this is a Cypher-style CREATE (not legacy CREATE NODE/REL)
        let is_cypher_create = upper.starts_with("CREATE") 
            && trimmed.len() > 6 
            && trimmed[6..].trim_start().starts_with('(');
        
        if is_cypher_create {
            create_batch.push(trimmed.to_string());
        } else {
            // Flush any accumulated CREATE batch
            if !create_batch.is_empty() {
                // Join with newline so parser sees multiple CREATE clauses
                result.push(create_batch.join("\n"));
                create_batch.clear();
            }
            result.push(trimmed.to_string());
        }
    }
    
    // Flush remaining CREATE batch
    if !create_batch.is_empty() {
        result.push(create_batch.join("\n"));
    }
    
    result
}

/// Extract label from a MATCH clause like "MATCH (n:Person)" -> Some("Person")
fn extract_match_label(upper_query: &str) -> Option<String> {
    // Look for pattern like "MATCH (var:Label)" or "MATCH (:Label)"
    if let Some(match_idx) = upper_query.find("MATCH") {
        let after_match = &upper_query[match_idx + 5..];
        if let Some(paren_idx) = after_match.find('(') {
            let after_paren = &after_match[paren_idx + 1..];
            // Find the colon that indicates a label
            if let Some(colon_idx) = after_paren.find(':') {
                let after_colon = &after_paren[colon_idx + 1..];
                // Extract label until ) or space or {
                let label_end = after_colon.find(|c: char| c == ')' || c == ' ' || c == '{' || c == '-' || c == ']')
                    .unwrap_or(after_colon.len());
                let label = after_colon[..label_end].trim();
                if !label.is_empty() {
                    return Some(label.to_string());
                }
            }
        }
    }
    None
}

pub fn execute_query(db: &mut GraphDatabase, query: &str) -> Result<QueryOutcome> {
    // Normalize line endings: convert CRLF to LF and remove stray CR
    let normalized = query.replace("\r\n", "\n").replace('\r', "\n");
    let mut trimmed = normalized.trim();
    
    // Strip outer quotes if the entire query is wrapped in them (common from CLI)
    if (trimmed.starts_with('"') && trimmed.ends_with('"')) || 
       (trimmed.starts_with('\'') && trimmed.ends_with('\'')) {
        if trimmed.len() >= 2 {
            trimmed = &trimmed[1..trimmed.len()-1];
            trimmed = trimmed.trim();
        }
    }
    
    if trimmed.is_empty() {
        return Err(anyhow!("empty query"));
    }

    // We allow multiple statements separated by semicolons; execute sequentially
    let mut outcome = QueryOutcome::default();
    let mut any_mut = false;
    let statements = _split_statements(trimmed);
    
    // Batch consecutive CREATE statements so variable bindings persist across them
    // e.g., CREATE (p1:Person) followed by CREATE (p1)-[:REL]->(c1) needs p1 bound
    let batched_statements = batch_consecutive_creates(&statements);
    
    for stmt in batched_statements {
        let stmt = stmt.trim();
        if stmt.is_empty() { continue; }
        let upper = stmt.to_uppercase();
        let res = if upper.starts_with("MATCH (") && upper.contains(" MERGE ") {
            // Legacy minimal Cypher-style pairwise support (kept for compatibility)
            exec_cypher_match_merge(db, stmt)
        // If the statement appears to be OpenCypher, route to the Cypher engine.
        // Detect by keywords and forms that are NOT the legacy custom commands.
        } else if (upper.starts_with("MATCH") && !upper.contains("CALL ") && (stmt[5..].trim_start().starts_with('(') || stmt[5..].trim_start().starts_with("(") || (stmt[5..].trim_start().contains('.') && !stmt[5..].trim_start().starts_with("NODE ") && !stmt[5..].trim_start().starts_with("REL ")))) ||
        // WITH is Cypher-only
        upper.starts_with("WITH ") ||
        // UNWIND is Cypher-only
        upper.starts_with("UNWIND ") ||
        // DETACH DELETE is Cypher-only
        upper.starts_with("DETACH DELETE ") ||
        // OPTIONAL MATCH with '(' or shorthand
        (upper.starts_with("OPTIONAL MATCH ") && (stmt[15..].trim_start().starts_with('(') || (stmt[15..].trim_start().contains('.') && !stmt[15..].trim_start().starts_with("NODE ") && !stmt[15..].trim_start().starts_with("REL ")))) ||
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
        (upper.starts_with("CREATE") && stmt[6..].trim_start().starts_with('(')) ||
        // Catch-all for multi-clause queries (but NOT if it contains CALL - those need special handling)
        (upper.contains("WITH ") && (upper.contains("MATCH ") || upper.contains("RETURN ")) && !upper.contains("CALL ")) ||
        (upper.contains("UNWIND ") && upper.contains("WITH ") && !upper.contains("CALL ")) {
            let rows = execute_cypher(db, stmt)?;
            // conservatively mark mutated if statement contains mutating keywords
            let mutated = upper.contains("CREATE")
                || upper.contains("MERGE")
                || upper.contains("SET ")
                || upper.contains("REMOVE ")
                || upper.contains("DELETE")
                || upper.contains("DETACH DELETE");
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
        } else if upper.starts_with("CALL ") {
            exec_call_procedure(db, &stmt[5..])
        } else if upper.contains("CALL ") {
            // Query contains CALL but doesn't start with it (e.g., "MATCH ... WITH ... CALL embedding.similar(...)")
            // Extract label filter from MATCH clause if present (e.g., "MATCH (n:Person)")
            let label_filter = extract_match_label(&upper);
            
            // Extract and execute just the CALL portion, injecting label filter if needed
            if let Some(call_idx) = upper.find("CALL ") {
                let call_part = &stmt[call_idx + 5..];
                exec_call_procedure_with_label(db, call_part, label_filter)
            } else {
                return Err(anyhow!("unrecognized statement: {}", stmt));
            }
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

        // Check if it's a "standard" Cypher query
        let is_cypher = (upper.starts_with("MATCH ") && stmt[6..].trim_start().starts_with('(')) ||
            (upper.starts_with("OPTIONAL MATCH ") && stmt[15..].trim_start().starts_with('(')) ||
            upper.starts_with("MERGE ") ||
            upper.starts_with("RETURN ") ||
            (upper.starts_with("DELETE ") && !upper.starts_with("DELETE NODE ") && !upper.starts_with("DELETE REL ")) ||
            upper.starts_with("DETACH DELETE ") ||
            (upper.starts_with("CREATE ") && stmt[7..].trim_start().starts_with('('));

        let res = if is_cypher {
            let rows = execute_cypher_with_params(db, stmt, params)?;
            let mutated = upper.starts_with("CREATE ") || upper.starts_with("MERGE ") || 
                (upper.starts_with("DELETE ") && !upper.starts_with("DELETE NODE ") && !upper.starts_with("DELETE REL ")) || 
                upper.starts_with("DETACH DELETE ");
            Ok((rows, 0, 0, mutated))
        } else if upper.starts_with("MATCH (") && upper.contains(" MERGE ") {
            exec_cypher_match_merge(db, stmt)
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
            // Default to Cypher engine if it doesn't match legacy custom commands
            let rows = execute_cypher_with_params(db, stmt, params)?;
            let mutated = upper.starts_with("CREATE ") || upper.starts_with("MERGE ") || 
                (upper.starts_with("DELETE ") && !upper.starts_with("DELETE NODE ") && !upper.starts_with("DELETE REL ")) || 
                upper.starts_with("DETACH DELETE ");
            Ok((rows, 0, 0, mutated))
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

/// Execute a CALL procedure with an optional label filter from preceding MATCH clause
fn exec_call_procedure_with_label(
    db: &mut GraphDatabase,
    rest: &str,
    label_filter: Option<String>,
) -> Result<(Vec<QueryResultRow>, usize, usize, bool)> {
    let upper = rest.to_uppercase();
    
    // For embedding.similar and embedding.threshold, inject label filter if not already provided
    if upper.starts_with("EMBEDDING.SIMILAR") || upper.starts_with("EMBEDDING.THRESHOLD") {
        if let Some(label) = label_filter {
            // Check if a label filter is already provided (3rd argument for similar, would need different handling)
            let args = extract_call_args(rest)?;
            if upper.starts_with("EMBEDDING.SIMILAR") && args.len() == 2 {
                // Inject label as 3rd argument: embedding.similar("query", k, "Label")
                let new_call = format!("embedding.similar(\"{}\", {}, \"{}\")", args[0], args[1], label);
                return exec_call_procedure(db, &new_call);
            }
        }
    }
    
    // Fall back to normal procedure execution
    exec_call_procedure(db, rest)
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
        ids = ids.into_par_iter().filter(|id| {
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
        }).collect();
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
        ids = ids.into_par_iter().filter(|rid| {
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
        }).collect();
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

/// Execute CALL procedure for graph algorithms
/// Supported: CALL algo.pageRank(), CALL algo.betweenness(), CALL algo.shortestPath(from, to), CALL algo.allPaths(from, to, maxDepth)
fn exec_call_procedure(db: &mut GraphDatabase, rest: &str) -> Result<(Vec<QueryResultRow>, usize, usize, bool)> {
    let rest = rest.trim();
    let upper = rest.to_uppercase();
    
    if upper.starts_with("ALGO.PAGERANK") {
        // CALL algo.pageRank() or CALL algo.pageRank(damping, iterations)
        let scores = pagerank(db, 0.85, 20);
        let mut rows: Vec<QueryResultRow> = scores.iter()
            .filter_map(|(id, score)| {
                db.get_node(*id).map(|n| {
                    let mut meta = n.metadata.clone();
                    meta.insert("_score".to_string(), format!("{:.6}", score));
                    QueryResultRow::Node { id: *id, label: n.label.clone(), metadata: meta }
                })
            })
            .collect();
        // Sort by score descending
        rows.sort_by(|a, b| {
            let score_a = match a { QueryResultRow::Node { metadata, .. } => metadata.get("_score").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0), _ => 0.0 };
            let score_b = match b { QueryResultRow::Node { metadata, .. } => metadata.get("_score").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0), _ => 0.0 };
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("ALGO.BETWEENNESS") {
        // CALL algo.betweenness()
        let scores = betweenness_centrality(db);
        let mut rows: Vec<QueryResultRow> = scores.iter()
            .filter_map(|(id, score)| {
                db.get_node(*id).map(|n| {
                    let mut meta = n.metadata.clone();
                    meta.insert("_score".to_string(), format!("{:.6}", score));
                    QueryResultRow::Node { id: *id, label: n.label.clone(), metadata: meta }
                })
            })
            .collect();
        rows.sort_by(|a, b| {
            let score_a = match a { QueryResultRow::Node { metadata, .. } => metadata.get("_score").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0), _ => 0.0 };
            let score_b = match b { QueryResultRow::Node { metadata, .. } => metadata.get("_score").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0), _ => 0.0 };
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("ALGO.SHORTESTPATH") {
        // CALL algo.shortestPath(fromId, toId)
        let args = extract_call_args(rest)?;
        if args.len() < 2 {
            return Err(anyhow!("algo.shortestPath requires 2 arguments: fromId, toId"));
        }
        let from_id = Uuid::parse_str(&args[0]).map_err(|e| anyhow!("invalid from uuid: {}", e))?;
        let to_id = Uuid::parse_str(&args[1]).map_err(|e| anyhow!("invalid to uuid: {}", e))?;
        
        match shortest_path(db, from_id, to_id) {
            Some(path) => {
                let rows: Vec<QueryResultRow> = path.iter()
                    .enumerate()
                    .filter_map(|(i, id)| {
                        db.get_node(*id).map(|n| {
                            let mut meta = n.metadata.clone();
                            meta.insert("_pathIndex".to_string(), i.to_string());
                            QueryResultRow::Node { id: *id, label: n.label.clone(), metadata: meta }
                        })
                    })
                    .collect();
                Ok((rows, 0, 0, false))
            }
            None => Ok((vec![QueryResultRow::Info("No path found".to_string())], 0, 0, false))
        }
    } else if upper.starts_with("ALGO.ASTAR") {
        // CALL algo.astar(fromId, toId) - requires node positions, uses Euclidean heuristic
        // Note: This requires positions which aren't available in query context, so we fall back to BFS
        let args = extract_call_args(rest)?;
        if args.len() < 2 {
            return Err(anyhow!("algo.astar requires 2 arguments: fromId, toId"));
        }
        let from_id = Uuid::parse_str(&args[0]).map_err(|e| anyhow!("invalid from uuid: {}", e))?;
        let to_id = Uuid::parse_str(&args[1]).map_err(|e| anyhow!("invalid to uuid: {}", e))?;
        
        // Use empty positions - astar will fall back to BFS behavior
        let positions = std::collections::HashMap::new();
        match astar_path(db, &positions, from_id, to_id) {
            Some(path) => {
                let rows: Vec<QueryResultRow> = path.iter()
                    .enumerate()
                    .filter_map(|(i, id)| {
                        db.get_node(*id).map(|n| {
                            let mut meta = n.metadata.clone();
                            meta.insert("_pathIndex".to_string(), i.to_string());
                            QueryResultRow::Node { id: *id, label: n.label.clone(), metadata: meta }
                        })
                    })
                    .collect();
                Ok((rows, 0, 0, false))
            }
            None => Ok((vec![QueryResultRow::Info("No path found".to_string())], 0, 0, false))
        }
    } else if upper.starts_with("ALGO.ALLPATHS") {
        // CALL algo.allPaths(fromId, toId, maxDepth)
        let args = extract_call_args(rest)?;
        if args.len() < 3 {
            return Err(anyhow!("algo.allPaths requires 3 arguments: fromId, toId, maxDepth"));
        }
        let from_id = Uuid::parse_str(&args[0]).map_err(|e| anyhow!("invalid from uuid: {}", e))?;
        let to_id = Uuid::parse_str(&args[1]).map_err(|e| anyhow!("invalid to uuid: {}", e))?;
        let max_depth: usize = args[2].parse().map_err(|_| anyhow!("invalid maxDepth"))?;
        
        let paths = all_paths(db, from_id, to_id, max_depth);
        let mut rows = Vec::new();
        for (path_idx, path) in paths.iter().enumerate() {
            let path_str = path.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(" -> ");
            rows.push(QueryResultRow::Info(format!("Path {}: {} (length {})", path_idx + 1, path_str, path.len())));
        }
        if rows.is_empty() {
            rows.push(QueryResultRow::Info("No paths found".to_string()));
        }
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("DB.SEARCH") {
        // CALL db.search("query text") - Full-text search using FTS5
        let args = extract_call_args(rest)?;
        if args.is_empty() {
            return Err(anyhow!("db.search requires 1 argument: search query"));
        }
        let query = &args[0];
        
        // Search in-memory by label and metadata (FTS5 is in SQLite, this is a fallback)
        let mut rows = Vec::new();
        let query_lower = query.to_lowercase();
        for node in db.nodes.values() {
            let label_match = node.label.to_lowercase().contains(&query_lower);
            let meta_match = node.metadata.values().any(|v| v.to_lowercase().contains(&query_lower));
            if label_match || meta_match {
                rows.push(QueryResultRow::Node {
                    id: node.id,
                    label: node.label.clone(),
                    metadata: node.metadata.clone(),
                });
            }
        }
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("TEMPORAL.TIMELINE") {
        // CALL temporal.timeline() - Get timeline of all graph events
        let timeline = get_timeline(db);
        let rows: Vec<QueryResultRow> = timeline.iter()
            .map(|e| QueryResultRow::Info(format!("{}: {:?} {} ({})", e.timestamp, e.event_type, e.entity_id, e.label)))
            .collect();
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("TEMPORAL.RANGE") {
        // CALL temporal.range() - Get timestamp range of the graph
        match get_timestamp_range(db) {
            Some((min, max)) => Ok((vec![QueryResultRow::Info(format!("min: {}, max: {}", min, max))], 0, 0, false)),
            None => Ok((vec![QueryResultRow::Info("Graph is empty".to_string())], 0, 0, false)),
        }
    } else if upper.starts_with("TEMPORAL.NODESINRANGE") {
        // CALL temporal.nodesInRange(fromTimestamp, toTimestamp)
        let args = extract_call_args(rest)?;
        if args.len() < 2 {
            return Err(anyhow!("temporal.nodesInRange requires 2 arguments: fromTimestamp, toTimestamp"));
        }
        let from: i64 = args[0].parse().map_err(|_| anyhow!("invalid fromTimestamp"))?;
        let to: i64 = args[1].parse().map_err(|_| anyhow!("invalid toTimestamp"))?;
        let node_ids = nodes_in_range(db, from, to);
        let rows: Vec<QueryResultRow> = node_ids.iter()
            .filter_map(|id| db.get_node(*id).map(|n| QueryResultRow::Node {
                id: *id,
                label: n.label.clone(),
                metadata: n.metadata.clone(),
            }))
            .collect();
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("TEMPORAL.ATTIME") {
        // CALL temporal.atTime(timestamp) - Get graph state at a specific time
        let args = extract_call_args(rest)?;
        if args.is_empty() {
            return Err(anyhow!("temporal.atTime requires 1 argument: timestamp"));
        }
        let timestamp: i64 = args[0].parse().map_err(|_| anyhow!("invalid timestamp"))?;
        let (nodes, rels) = graph_at_time(db, timestamp);
        let mut rows: Vec<QueryResultRow> = nodes.iter()
            .map(|n| QueryResultRow::Node { id: n.id, label: n.label.clone(), metadata: n.metadata.clone() })
            .collect();
        for r in &rels {
            rows.push(QueryResultRow::Relationship {
                id: r.id,
                from: r.from_node,
                to: r.to_node,
                label: r.label.clone(),
                metadata: r.metadata.clone(),
            });
        }
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("DB.SCHEMA") {
        // CALL db.schema() - Get graph schema (labels and relationship types)
        use crate::semantic::rag::suggest_queries;
        let suggestions = suggest_queries(db);
        let mut rows = Vec::new();
        
        // List node labels
        for (label, ids) in &db.label_index {
            rows.push(QueryResultRow::Info(format!("Label: {} ({} nodes)", label, ids.len())));
        }
        
        // List relationship types
        let mut rel_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for rel in db.relationships.values() {
            *rel_counts.entry(rel.label.clone()).or_insert(0) += 1;
        }
        for (label, count) in rel_counts {
            rows.push(QueryResultRow::Info(format!("RelType: {} ({} relationships)", label, count)));
        }
        
        // Add query suggestions
        rows.push(QueryResultRow::Info("--- Suggested Queries ---".to_string()));
        for suggestion in suggestions.iter().take(5) {
            rows.push(QueryResultRow::Info(suggestion.clone()));
        }
        
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("SEMANTIC.EXTRACT") {
        // CALL semantic.extract("text") - Extract entities from text (simple heuristic mode)
        use crate::semantic::extraction::extract_entities_simple;
        let args = extract_call_args(rest)?;
        if args.is_empty() {
            return Err(anyhow!("semantic.extract requires 1 argument: text"));
        }
        let text = &args[0];
        let result = extract_entities_simple(text);
        
        let mut rows = Vec::new();
        for entity in &result.entities {
            rows.push(QueryResultRow::Info(format!(
                "Entity: {} [{}] (confidence: {:.2})",
                entity.name, entity.label, entity.confidence
            )));
        }
        if rows.is_empty() {
            rows.push(QueryResultRow::Info("No entities extracted".to_string()));
        }
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("EMBEDDING.SIMILAR") {
        // CALL embedding.similar("query text", k, "Label") - Find k most similar nodes by embedding
        // Optional 3rd argument filters by node label
        use crate::semantic::embeddings::{UnifiedEmbedder, cosine_similarity, l2_distance, NearestNeighbor};
        use crate::persistence::settings::AppSettings;
        use crate::persistence::persist::get_current_embedding_model;
        
        let args = extract_call_args(rest)?;
        if args.is_empty() {
            return Err(anyhow!("embedding.similar requires at least 1 argument: query text"));
        }
        let query_text = &args[0];
        let k: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
        let label_filter: Option<String> = args.get(2).map(|s| s.to_string());
        
        // Load model from SQLite first, fall back to settings file
        let model = get_current_embedding_model()
            .unwrap_or_else(|| AppSettings::load().map(|s| s.embedding_model).unwrap_or_default());
        
        // Create embedder for the configured model
        let mut embedder = UnifiedEmbedder::new(model)
            .map_err(|e| anyhow!("Failed to initialize embedder: {}", e))?;
        
        // For TF-IDF/Word2Vec, we MUST fit on the corpus first so the query embedding
        // uses the same vocabulary as the stored embeddings
        if matches!(model, crate::persistence::settings::EmbeddingModel::TfIdf | crate::persistence::settings::EmbeddingModel::Word2Vec) {
            let texts: Vec<String> = db.nodes.values()
                .map(|n| {
                    let mut parts = vec![n.label.clone()];
                    parts.extend(n.metadata.values().cloned());
                    parts.join(" ")
                })
                .collect();
            embedder.fit(&texts);
        }
        
        let query_embedding = embedder.embed(query_text);
        
        // Always generate fresh embeddings for comparison to ensure consistency
        // This guarantees the query and node embeddings use the same model state
        // Filter by label if provided
        let mut neighbors: Vec<NearestNeighbor> = db.nodes.iter()
            .filter(|(_, node)| {
                if let Some(ref label) = label_filter {
                    node.label.eq_ignore_ascii_case(label)
                } else {
                    true
                }
            })
            .map(|(id, node)| {
                let mut parts = vec![node.label.clone()];
                parts.extend(node.metadata.values().cloned());
                let text = parts.join(" ");
                let emb = embedder.embed(&text);
                let sim = cosine_similarity(&query_embedding, &emb);
                let dist = l2_distance(&query_embedding, &emb);
                NearestNeighbor { node_id: *id, similarity: sim, distance: dist }
            })
            .collect();
        neighbors.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        neighbors.truncate(k);
        let results = neighbors;
        
        let rows: Vec<QueryResultRow> = results.iter()
            .filter_map(|nn| {
                db.get_node(nn.node_id).map(|n| {
                    let mut meta = n.metadata.clone();
                    meta.insert("_similarity".to_string(), format!("{:.4}", nn.similarity));
                    meta.insert("_distance".to_string(), format!("{:.4}", nn.distance));
                    QueryResultRow::Node { id: nn.node_id, label: n.label.clone(), metadata: meta }
                })
            })
            .collect();
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("EMBEDDING.NEIGHBORS") {
        // CALL embedding.neighbors(nodeId, k) - Find k nearest neighbors to a node
        use crate::semantic::embeddings::{cosine_similarity, l2_distance, NearestNeighbor};
        use crate::persistence::settings::AppSettings;
        use crate::persistence::persist::get_current_embedding_model;
        
        let args = extract_call_args(rest)?;
        if args.is_empty() {
            return Err(anyhow!("embedding.neighbors requires at least 1 argument: nodeId"));
        }
        let node_id = Uuid::parse_str(&args[0]).map_err(|e| anyhow!("invalid nodeId: {}", e))?;
        let k: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
        
        // Load model from SQLite first, fall back to settings file
        let model = get_current_embedding_model()
            .unwrap_or_else(|| AppSettings::load().map(|s| s.embedding_model).unwrap_or_default());
        let model_type = match model {
            crate::persistence::settings::EmbeddingModel::TfIdf => "tfidf",
            crate::persistence::settings::EmbeddingModel::Word2Vec => "word2vec",
            crate::persistence::settings::EmbeddingModel::Onnx => "onnx",
        };
        
        // Load persisted embeddings from SQLite
        let stored_embeddings = if let Ok(storage) = get_embedding_storage() {
            storage.load_all_model_embeddings(model_type).unwrap_or_default()
        } else {
            HashMap::new()
        };
        
        let results: Vec<NearestNeighbor> = if let Some(source_emb) = stored_embeddings.get(&node_id) {
            let mut neighbors: Vec<NearestNeighbor> = stored_embeddings.iter()
                .filter(|(id, _)| **id != node_id)
                .map(|(id, emb)| {
                    let sim = cosine_similarity(source_emb, emb);
                    let dist = l2_distance(source_emb, emb);
                    NearestNeighbor { node_id: *id, similarity: sim, distance: dist }
                })
                .collect();
            neighbors.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
            neighbors.truncate(k);
            neighbors
        } else {
            Vec::new()
        };
        
        let rows: Vec<QueryResultRow> = results.iter()
            .filter_map(|nn| {
                db.get_node(nn.node_id).map(|n| {
                    let mut meta = n.metadata.clone();
                    meta.insert("_similarity".to_string(), format!("{:.4}", nn.similarity));
                    meta.insert("_distance".to_string(), format!("{:.4}", nn.distance));
                    QueryResultRow::Node { id: nn.node_id, label: n.label.clone(), metadata: meta }
                })
            })
            .collect();
        Ok((rows, 0, 0, false))
    } else if upper.starts_with("EMBEDDING.THRESHOLD") {
        // CALL embedding.threshold("query text", threshold) - Find nodes above similarity threshold
        use crate::semantic::embeddings::{UnifiedEmbedder, cosine_similarity, l2_distance, NearestNeighbor};
        use crate::persistence::settings::AppSettings;
        use crate::persistence::persist::get_current_embedding_model;
        
        let args = extract_call_args(rest)?;
        if args.len() < 2 {
            return Err(anyhow!("embedding.threshold requires 2 arguments: query text, threshold (0.0-1.0)"));
        }
        let query_text = &args[0];
        let threshold: f32 = args[1].parse().map_err(|_| anyhow!("invalid threshold"))?;
        
        // Load model from SQLite first, fall back to settings file
        let model = get_current_embedding_model()
            .unwrap_or_else(|| AppSettings::load().map(|s| s.embedding_model).unwrap_or_default());
        
        // Create embedder for the configured model
        let mut embedder = UnifiedEmbedder::new(model)
            .map_err(|e| anyhow!("Failed to initialize embedder: {}", e))?;
        
        // For TF-IDF/Word2Vec, we MUST fit on the corpus first
        if matches!(model, crate::persistence::settings::EmbeddingModel::TfIdf | crate::persistence::settings::EmbeddingModel::Word2Vec) {
            let texts: Vec<String> = db.nodes.values()
                .map(|n| {
                    let mut parts = vec![n.label.clone()];
                    parts.extend(n.metadata.values().cloned());
                    parts.join(" ")
                })
                .collect();
            embedder.fit(&texts);
        }
        
        let query_embedding = embedder.embed(query_text);
        
        // Always generate fresh embeddings for comparison to ensure consistency
        let mut neighbors: Vec<NearestNeighbor> = db.nodes.iter()
            .filter_map(|(id, node)| {
                let mut parts = vec![node.label.clone()];
                parts.extend(node.metadata.values().cloned());
                let text = parts.join(" ");
                let emb = embedder.embed(&text);
                let sim = cosine_similarity(&query_embedding, &emb);
                if sim >= threshold {
                    Some(NearestNeighbor { node_id: *id, similarity: sim, distance: l2_distance(&query_embedding, &emb) })
                } else {
                    None
                }
            })
            .collect();
        neighbors.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        let results = neighbors;
        
        let rows: Vec<QueryResultRow> = results.iter()
            .filter_map(|nn| {
                db.get_node(nn.node_id).map(|n| {
                    let mut meta = n.metadata.clone();
                    meta.insert("_similarity".to_string(), format!("{:.4}", nn.similarity));
                    meta.insert("_distance".to_string(), format!("{:.4}", nn.distance));
                    QueryResultRow::Node { id: nn.node_id, label: n.label.clone(), metadata: meta }
                })
            })
            .collect();
        Ok((rows, 0, 0, false))
    } else {
        Err(anyhow!("unknown procedure: {}", rest))
    }
}

/// Extract arguments from a CALL procedure invocation like "algo.pageRank(arg1, arg2)"
/// Properly handles quoted strings containing commas or spaces.
fn extract_call_args(s: &str) -> Result<Vec<String>> {
    if let Some(start) = s.find('(') {
        if let Some(end) = s.rfind(')') {
            let inner = s[start+1..end].trim();
            if inner.is_empty() {
                return Ok(Vec::new());
            }
            
            // Parse arguments respecting quoted strings
            let mut args = Vec::new();
            let mut current = String::new();
            let mut in_quote: Option<char> = None;
            
            for c in inner.chars() {
                match c {
                    '"' | '\'' => {
                        if in_quote == Some(c) {
                            // End of quoted string
                            in_quote = None;
                        } else if in_quote.is_none() {
                            // Start of quoted string
                            in_quote = Some(c);
                        } else {
                            // Different quote inside a quoted string
                            current.push(c);
                        }
                    }
                    ',' if in_quote.is_none() => {
                        // Argument separator outside quotes
                        let arg = current.trim().to_string();
                        if !arg.is_empty() {
                            args.push(arg);
                        }
                        current.clear();
                    }
                    _ => {
                        current.push(c);
                    }
                }
            }
            
            // Don't forget the last argument
            let arg = current.trim().to_string();
            if !arg.is_empty() {
                args.push(arg);
            }
            
            return Ok(args);
        }
    }
    Ok(Vec::new())
}

/// Re-embed all nodes using the specified embedding model.
/// Only clears and rebuilds embeddings for the specified model, preserving other models' embeddings.
pub fn reembed_with_model(db: &GraphDatabase, model: crate::persistence::settings::EmbeddingModel) -> Result<String> {
    use crate::semantic::embeddings::UnifiedEmbedder;
    
    let node_count = db.nodes.len();
    if node_count == 0 {
        return Ok("No nodes to embed".to_string());
    }

    // Create embedder for the specified model
    let mut embedder = UnifiedEmbedder::new(model)
        .map_err(|e| anyhow!("Failed to initialize embedder: {}", e))?;

    // Collect all text from nodes
    let mut texts: Vec<String> = Vec::new();
    let mut node_ids: Vec<NodeId> = Vec::new();
    
    for (id, node) in &db.nodes {
        let mut text_parts = vec![node.label.clone()];
        for value in node.metadata.values() {
            text_parts.push(value.clone());
        }
        texts.push(text_parts.join(" "));
        node_ids.push(*id);
    }

    // Fit the embedder (for TF-IDF models)
    embedder.fit(&texts);

    // Generate embeddings
    let embeddings = embedder.embed_batch(&texts);
    
    let model_type = embedder.model_type_str();
    
    // Store embeddings in SQLite if available (using per-model tables)
    if let Ok(mut storage) = get_embedding_storage() {
        // Clear only this model's embeddings (not all embeddings)
        let _ = storage.clear_model_embeddings(model_type);
        
        // Save model state (for TF-IDF) with model-specific key
        if let Some(state) = embedder.serialize_state() {
            let state_key = format!("{}_state", model_type);
            let _ = storage.save_embedding_model_state(&state_key, &state);
        }
        
        // Batch save embeddings for better performance
        let embedding_pairs: Vec<(NodeId, Vec<f32>)> = node_ids.iter()
            .zip(embeddings.iter())
            .map(|(id, emb)| (*id, emb.clone()))
            .collect();
        let _ = storage.save_model_embeddings_batch(model_type, &embedding_pairs);
    }

    let model_name = match model {
        crate::persistence::settings::EmbeddingModel::TfIdf => "TF-IDF",
        crate::persistence::settings::EmbeddingModel::Word2Vec => "Word2Vec",
        crate::persistence::settings::EmbeddingModel::Onnx => "ONNX (all-MiniLM-L6-v2)",
    };

    Ok(format!("Re-embedded {} nodes using {}", node_count, model_name))
}

/// Get SQLite storage for embeddings
fn get_embedding_storage() -> Result<crate::persistence::sqlite_backend::SqliteStorage> {
    let path = crate::persistence::persist::active_sqlite_path();
    crate::persistence::sqlite_backend::SqliteStorage::open(&path)
        .map_err(|e| anyhow!("Failed to open SQLite storage: {}", e))
}
