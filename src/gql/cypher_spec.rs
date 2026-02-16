use anyhow::{anyhow, Result};
use std::collections::HashMap;
use uuid::Uuid;

use crate::graph_utils::graph::{GraphDatabase, Node, Relationship};
use super::query_interface::QueryResultRow;

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum Expr {
    Var(String),
    Prop(Box<Expr>, String),
    FuncId(String),
    FuncTimestamp(String),
    FuncCollect(String),          // collect(var)
    #[allow(dead_code)]
    ListSlice(String, Option<usize>, Option<usize>), // var[start..end]
    Str(String),
    Num(f64),                     // numeric literal
    Bool(bool),                   // boolean literal
    Null,                         // null literal
    Alias(Box<Expr>, String),     // expr AS alias
    // Aggregation functions
    FuncCount(Box<Expr>),         // count(expr) or count(*)
    FuncCountDistinct(Box<Expr>), // count(DISTINCT expr)
    FuncSum(Box<Expr>),           // sum(expr)
    FuncAvg(Box<Expr>),           // avg(expr)
    FuncMin(Box<Expr>),           // min(expr)
    FuncMax(Box<Expr>),           // max(expr)
    // String functions
    FuncToUpper(Box<Expr>),       // toUpper(expr)
    FuncToLower(Box<Expr>),       // toLower(expr)
    FuncTrim(Box<Expr>),          // trim(expr)
    FuncLTrim(Box<Expr>),         // ltrim(expr)
    FuncRTrim(Box<Expr>),         // rtrim(expr)
    FuncReplace(Box<Expr>, Box<Expr>, Box<Expr>), // replace(str, search, replacement)
    FuncSubstring(Box<Expr>, Box<Expr>, Option<Box<Expr>>), // substring(str, start, [length])
    FuncLeft(Box<Expr>, Box<Expr>),  // left(str, n)
    FuncRight(Box<Expr>, Box<Expr>), // right(str, n)
    FuncSplit(Box<Expr>, Box<Expr>), // split(str, delimiter)
    FuncReverse(Box<Expr>),       // reverse(str)
    FuncSize(Box<Expr>),          // size(str) or size(list)
    // Type conversion functions
    FuncToInteger(Box<Expr>),     // toInteger(expr)
    FuncToFloat(Box<Expr>),       // toFloat(expr)
    FuncToString(Box<Expr>),      // toString(expr)
    FuncToBoolean(Box<Expr>),     // toBoolean(expr)
    // List functions
    FuncHead(Box<Expr>),          // head(list)
    FuncTail(Box<Expr>),          // tail(list)
    FuncLast(Box<Expr>),          // last(list)
    FuncRange(Box<Expr>, Box<Expr>, Option<Box<Expr>>), // range(start, end, [step])
    FuncKeys(Box<Expr>),          // keys(node/rel)
    FuncLabels(Box<Expr>),        // labels(node)
    FuncType(Box<Expr>),          // type(rel)
    FuncNodes(Box<Expr>),         // nodes(path)
    FuncRelationships(Box<Expr>), // relationships(path)
    // Math functions
    FuncAbs(Box<Expr>),           // abs(expr)
    FuncCeil(Box<Expr>),          // ceil(expr)
    FuncFloor(Box<Expr>),         // floor(expr)
    FuncRound(Box<Expr>),         // round(expr)
    FuncSign(Box<Expr>),          // sign(expr)
    FuncRand,                     // rand()
    FuncSqrt(Box<Expr>),          // sqrt(expr)
    FuncLog(Box<Expr>),           // log(expr)
    FuncLog10(Box<Expr>),         // log10(expr)
    FuncExp(Box<Expr>),           // exp(expr)
    FuncPow(Box<Expr>, Box<Expr>),// pow(base, exp) or base ^ exp
    // Predicate functions
    FuncExists(Box<Expr>),        // exists(expr)
    FuncCoalesce(Vec<Expr>),      // coalesce(expr1, expr2, ...)
    // CASE expression
    Case { operand: Option<Box<Expr>>, when_clauses: Vec<(Expr, Expr)>, else_clause: Option<Box<Expr>> },
    // List literal
    List(Vec<Expr>),
    // Map literal
    Map(Vec<(String, Expr)>),
    // Binary operations (for complex expressions)
    BinOp { left: Box<Expr>, op: BinOperator, right: Box<Expr> },
    // Unary NOT
    Not(Box<Expr>),
    // IS NULL / IS NOT NULL
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    // IN operator
    In(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum BinOperator {
    Add, Sub, Mul, Div, Mod,      // arithmetic
    Eq, Ne, Lt, Le, Gt, Ge,       // comparison
    And, Or, Xor,                 // logical
    RegexMatch,                   // =~
    Concat,                       // + for strings
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
    #[allow(dead_code)]
    Unwind { expr: String, var: String }, // UNWIND expr AS var
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
    // Split by commas not inside braces, parentheses, brackets, or quotes
    let mut out = Vec::new();
    let mut brace_level = 0i32;
    let mut paren_level = 0i32;
    let mut bracket_level = 0i32;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        let c = b as char;
        match c {
            '\'' if !in_dq => in_sq = !in_sq,
            '"' if !in_sq => in_dq = !in_dq,
            '{' if !in_sq && !in_dq => brace_level += 1,
            '}' if !in_sq && !in_dq => brace_level -= 1,
            '(' if !in_sq && !in_dq => paren_level += 1,
            ')' if !in_sq && !in_dq => paren_level -= 1,
            '[' if !in_sq && !in_dq => bracket_level += 1,
            ']' if !in_sq && !in_dq => bracket_level -= 1,
            ',' if brace_level == 0 && paren_level == 0 && bracket_level == 0 && !in_sq && !in_dq => {
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
    for part in split_top_level_comma(s) {
        let p = part.trim();
        if p.is_empty() { continue; }
        let pu = p.to_uppercase();
        
        // Check for AS alias
        let (expr_part, alias) = if let Some(as_idx) = find_keyword_boundary(&pu, "AS") {
            let expr_str = p[..as_idx].trim();
            let alias_str = p[as_idx+2..].trim().to_string();
            (expr_str, Some(alias_str))
        } else {
            (p, None)
        };
        
        let expr = parse_single_expr(expr_part)?;
        
        if let Some(alias_name) = alias {
            items.push(Expr::Alias(Box::new(expr), alias_name));
        } else {
            items.push(expr);
        }
    }
    Ok(items)
}

fn parse_single_expr(p: &str) -> Result<Expr> {
    let p = p.trim();
    if p.is_empty() { return Err(anyhow!("empty expression")); }
    let pu = p.to_uppercase();
    
    // null literal
    if pu == "NULL" { return Ok(Expr::Null); }
    // boolean literals
    if pu == "TRUE" { return Ok(Expr::Bool(true)); }
    if pu == "FALSE" { return Ok(Expr::Bool(false)); }
    
    // CASE expression
    if pu.starts_with("CASE ") || pu.starts_with("CASE\n") || pu == "CASE" {
        return parse_case_expr(p);
    }
    
    // id(var)
    if pu.starts_with("ID(") && p.ends_with(')') {
        let v = p[3..p.len()-1].trim();
        return Ok(Expr::FuncId(v.to_string()));
    }
    // timestamp(var)
    if pu.starts_with("TIMESTAMP(") && p.ends_with(')') {
        let v = p[10..p.len()-1].trim();
        return Ok(Expr::FuncTimestamp(v.to_string()));
    }
    // collect(var)
    if pu.starts_with("COLLECT(") && p.ends_with(')') {
        let v = p[8..p.len()-1].trim();
        return Ok(Expr::FuncCollect(v.to_string()));
    }
    // count(*) or count(expr) or count(DISTINCT expr)
    if pu.starts_with("COUNT(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        let inner_up = inner.to_uppercase();
        if inner == "*" {
            return Ok(Expr::FuncCount(Box::new(Expr::Var("*".to_string()))));
        } else if inner_up.starts_with("DISTINCT ") {
            let expr = parse_single_expr(&inner[9..])?;
            return Ok(Expr::FuncCountDistinct(Box::new(expr)));
        } else {
            let expr = parse_single_expr(inner)?;
            return Ok(Expr::FuncCount(Box::new(expr)));
        }
    }
    // sum(expr)
    if pu.starts_with("SUM(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        return Ok(Expr::FuncSum(Box::new(parse_single_expr(inner)?)));
    }
    // avg(expr)
    if pu.starts_with("AVG(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        return Ok(Expr::FuncAvg(Box::new(parse_single_expr(inner)?)));
    }
    // min(expr)
    if pu.starts_with("MIN(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        return Ok(Expr::FuncMin(Box::new(parse_single_expr(inner)?)));
    }
    // max(expr)
    if pu.starts_with("MAX(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        return Ok(Expr::FuncMax(Box::new(parse_single_expr(inner)?)));
    }
    // String functions
    if pu.starts_with("TOUPPER(") && p.ends_with(')') {
        let inner = p[8..p.len()-1].trim();
        return Ok(Expr::FuncToUpper(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("TOLOWER(") && p.ends_with(')') {
        let inner = p[8..p.len()-1].trim();
        return Ok(Expr::FuncToLower(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("TRIM(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncTrim(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("LTRIM(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        return Ok(Expr::FuncLTrim(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("RTRIM(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        return Ok(Expr::FuncRTrim(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("REVERSE(") && p.ends_with(')') {
        let inner = p[8..p.len()-1].trim();
        return Ok(Expr::FuncReverse(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("SIZE(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncSize(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("REPLACE(") && p.ends_with(')') {
        let inner = p[8..p.len()-1].trim();
        let args = split_func_args(inner);
        if args.len() != 3 { return Err(anyhow!("replace() requires 3 arguments")); }
        return Ok(Expr::FuncReplace(
            Box::new(parse_single_expr(&args[0])?),
            Box::new(parse_single_expr(&args[1])?),
            Box::new(parse_single_expr(&args[2])?),
        ));
    }
    if pu.starts_with("SUBSTRING(") && p.ends_with(')') {
        let inner = p[10..p.len()-1].trim();
        let args = split_func_args(inner);
        if args.len() < 2 || args.len() > 3 { return Err(anyhow!("substring() requires 2 or 3 arguments")); }
        let len_expr = if args.len() == 3 { Some(Box::new(parse_single_expr(&args[2])?)) } else { None };
        return Ok(Expr::FuncSubstring(
            Box::new(parse_single_expr(&args[0])?),
            Box::new(parse_single_expr(&args[1])?),
            len_expr,
        ));
    }
    if pu.starts_with("LEFT(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        let args = split_func_args(inner);
        if args.len() != 2 { return Err(anyhow!("left() requires 2 arguments")); }
        return Ok(Expr::FuncLeft(
            Box::new(parse_single_expr(&args[0])?),
            Box::new(parse_single_expr(&args[1])?),
        ));
    }
    if pu.starts_with("RIGHT(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        let args = split_func_args(inner);
        if args.len() != 2 { return Err(anyhow!("right() requires 2 arguments")); }
        return Ok(Expr::FuncRight(
            Box::new(parse_single_expr(&args[0])?),
            Box::new(parse_single_expr(&args[1])?),
        ));
    }
    if pu.starts_with("SPLIT(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        let args = split_func_args(inner);
        if args.len() != 2 { return Err(anyhow!("split() requires 2 arguments")); }
        return Ok(Expr::FuncSplit(
            Box::new(parse_single_expr(&args[0])?),
            Box::new(parse_single_expr(&args[1])?),
        ));
    }
    // Type conversion functions
    if pu.starts_with("TOINTEGER(") && p.ends_with(')') {
        let inner = p[10..p.len()-1].trim();
        return Ok(Expr::FuncToInteger(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("TOFLOAT(") && p.ends_with(')') {
        let inner = p[8..p.len()-1].trim();
        return Ok(Expr::FuncToFloat(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("TOSTRING(") && p.ends_with(')') {
        let inner = p[9..p.len()-1].trim();
        return Ok(Expr::FuncToString(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("TOBOOLEAN(") && p.ends_with(')') {
        let inner = p[10..p.len()-1].trim();
        return Ok(Expr::FuncToBoolean(Box::new(parse_single_expr(inner)?)));
    }
    // List functions
    if pu.starts_with("HEAD(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncHead(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("TAIL(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncTail(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("LAST(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncLast(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("KEYS(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncKeys(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("LABELS(") && p.ends_with(')') {
        let inner = p[7..p.len()-1].trim();
        return Ok(Expr::FuncLabels(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("TYPE(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncType(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("RANGE(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        let args = split_func_args(inner);
        if args.len() < 2 || args.len() > 3 { return Err(anyhow!("range() requires 2 or 3 arguments")); }
        let step = if args.len() == 3 { Some(Box::new(parse_single_expr(&args[2])?)) } else { None };
        return Ok(Expr::FuncRange(
            Box::new(parse_single_expr(&args[0])?),
            Box::new(parse_single_expr(&args[1])?),
            step,
        ));
    }
    // Math functions
    if pu.starts_with("ABS(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        return Ok(Expr::FuncAbs(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("CEIL(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncCeil(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("FLOOR(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        return Ok(Expr::FuncFloor(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("ROUND(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        return Ok(Expr::FuncRound(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("SIGN(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncSign(Box::new(parse_single_expr(inner)?)));
    }
    if pu == "RAND()" { return Ok(Expr::FuncRand); }
    if pu.starts_with("SQRT(") && p.ends_with(')') {
        let inner = p[5..p.len()-1].trim();
        return Ok(Expr::FuncSqrt(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("LOG(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        return Ok(Expr::FuncLog(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("LOG10(") && p.ends_with(')') {
        let inner = p[6..p.len()-1].trim();
        return Ok(Expr::FuncLog10(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("EXP(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        return Ok(Expr::FuncExp(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("POW(") && p.ends_with(')') {
        let inner = p[4..p.len()-1].trim();
        let args = split_func_args(inner);
        if args.len() != 2 { return Err(anyhow!("pow() requires 2 arguments")); }
        return Ok(Expr::FuncPow(
            Box::new(parse_single_expr(&args[0])?),
            Box::new(parse_single_expr(&args[1])?),
        ));
    }
    // Predicate functions
    if pu.starts_with("EXISTS(") && p.ends_with(')') {
        let inner = p[7..p.len()-1].trim();
        return Ok(Expr::FuncExists(Box::new(parse_single_expr(inner)?)));
    }
    if pu.starts_with("COALESCE(") && p.ends_with(')') {
        let inner = p[9..p.len()-1].trim();
        let args = split_func_args(inner);
        let exprs: Result<Vec<Expr>> = args.iter().map(|a| parse_single_expr(a)).collect();
        return Ok(Expr::FuncCoalesce(exprs?));
    }
    // NOT prefix
    if pu.starts_with("NOT ") {
        let inner = p[4..].trim();
        return Ok(Expr::Not(Box::new(parse_single_expr(inner)?)));
    }
    // list slice: var[start..end] or var[start..] or var[..end]
    if let Some(bracket_start) = p.find('[') {
        if p.ends_with(']') {
            let var_name = p[..bracket_start].trim().to_string();
            let slice_part = &p[bracket_start+1..p.len()-1];
            if slice_part.contains("..") {
                let parts: Vec<&str> = slice_part.split("..").collect();
                let start = if parts[0].trim().is_empty() { None } else { Some(parts[0].trim().parse::<usize>().map_err(|_| anyhow!("invalid slice start"))?) };
                let end = if parts.len() < 2 || parts[1].trim().is_empty() { None } else { Some(parts[1].trim().parse::<usize>().map_err(|_| anyhow!("invalid slice end"))?) };
                return Ok(Expr::ListSlice(var_name, start, end));
            }
            // List literal [a, b, c]
            if var_name.is_empty() {
                let items = split_func_args(slice_part);
                let exprs: Result<Vec<Expr>> = items.iter().map(|a| parse_single_expr(a)).collect();
                return Ok(Expr::List(exprs?));
            }
        }
    }
    // List literal starting with [
    if p.starts_with('[') && p.ends_with(']') {
        let inner = &p[1..p.len()-1];
        let items = split_func_args(inner);
        let exprs: Result<Vec<Expr>> = items.iter().map(|a| parse_single_expr(a)).collect();
        return Ok(Expr::List(exprs?));
    }
    // var.prop
    if let Some(dot) = p.find('.') {
        let v = p[..dot].trim().to_string();
        let prop = p[dot+1..].trim().to_string();
        return Ok(Expr::Prop(Box::new(Expr::Var(v)), prop));
    }
    // string literal
    if p.starts_with('"') || p.starts_with('\'') { 
        return Ok(Expr::Str(trim_quotes(p)));
    }
    // numeric literal
    if let Ok(n) = p.parse::<f64>() {
        return Ok(Expr::Num(n));
    }
    // plain variable
    Ok(Expr::Var(p.to_string()))
}

/// Split function arguments respecting nested parentheses and quotes
fn split_func_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    
    for c in s.chars() {
        match c {
            '\'' if !in_dq => { in_sq = !in_sq; current.push(c); }
            '"' if !in_sq => { in_dq = !in_dq; current.push(c); }
            '(' | '[' | '{' if !in_sq && !in_dq => { depth += 1; current.push(c); }
            ')' | ']' | '}' if !in_sq && !in_dq => { depth -= 1; current.push(c); }
            ',' if depth == 0 && !in_sq && !in_dq => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        args.push(trimmed);
    }
    args
}

/// Parse CASE expression
fn parse_case_expr(p: &str) -> Result<Expr> {
    let pu = p.to_uppercase();
    // Simple CASE: CASE expr WHEN val THEN result [WHEN ...] [ELSE result] END
    // Searched CASE: CASE WHEN condition THEN result [WHEN ...] [ELSE result] END
    
    // Find END
    let end_idx = pu.rfind(" END").or_else(|| pu.rfind("\nEND")).ok_or_else(|| anyhow!("CASE missing END"))?;
    let body = &p[4..end_idx].trim(); // skip "CASE"
    let body_up = body.to_uppercase();
    
    // Check if it's a simple CASE (has operand before first WHEN)
    let first_when = body_up.find("WHEN").ok_or_else(|| anyhow!("CASE missing WHEN"))?;
    let operand_part = body[..first_when].trim();
    let operand = if operand_part.is_empty() {
        None
    } else {
        Some(Box::new(parse_single_expr(operand_part)?))
    };
    
    // Parse WHEN...THEN pairs and optional ELSE
    let mut when_clauses = Vec::new();
    let mut else_clause = None;
    let mut remaining = &body[first_when..];
    
    while !remaining.is_empty() {
        let rem_up = remaining.to_uppercase();
        if rem_up.starts_with("WHEN ") || rem_up.starts_with("WHEN\n") {
            // Find THEN
            let then_idx = rem_up.find(" THEN ").or_else(|| rem_up.find("\nTHEN "))
                .ok_or_else(|| anyhow!("WHEN missing THEN"))?;
            let when_expr = parse_single_expr(&remaining[5..then_idx].trim())?;
            
            // Find next WHEN or ELSE or end
            let after_then = &remaining[then_idx + 6..];
            let after_up = after_then.to_uppercase();
            let next_boundary = after_up.find(" WHEN ")
                .or_else(|| after_up.find("\nWHEN "))
                .or_else(|| after_up.find(" ELSE "))
                .or_else(|| after_up.find("\nELSE "))
                .unwrap_or(after_then.len());
            
            let then_expr = parse_single_expr(&after_then[..next_boundary].trim())?;
            when_clauses.push((when_expr, then_expr));
            remaining = &after_then[next_boundary..];
        } else if rem_up.starts_with("ELSE ") || rem_up.starts_with("ELSE\n") {
            let else_expr = parse_single_expr(&remaining[5..].trim())?;
            else_clause = Some(Box::new(else_expr));
            break;
        } else {
            remaining = &remaining[1..]; // skip whitespace
        }
    }
    
    Ok(Expr::Case { operand, when_clauses, else_clause })
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
        let expr_str = expr_str.trim();
        let expr_up = expr_str.to_uppercase();
        let expr = if expr_up.starts_with("ID(") && expr_str.ends_with(')') {
            let v = expr_str[3..expr_str.len()-1].trim();
            Expr::FuncId(v.to_string())
        } else if expr_up.starts_with("TIMESTAMP(") && expr_str.ends_with(')') {
            let v = expr_str[10..expr_str.len()-1].trim();
            Expr::FuncTimestamp(v.to_string())
        } else if let Some(dot) = expr_str.find('.') {
            let v = expr_str[..dot].trim().to_string();
            let prop = expr_str[dot+1..].trim().to_string();
            Expr::Prop(Box::new(Expr::Var(v)), prop)
        } else {
            Expr::Var(expr_str.to_string())
        };
        let _ = dir_part; // not used beyond detection
        out.push((expr, asc));
    }
    Ok(out)
}

fn parse(query: &str) -> Result<Vec<Clause>> {
    // Very small parser: MATCH ... [WHERE ...] RETURN ... | CREATE ... [RETURN ...] | MERGE ...
    // Normalize line endings: convert CRLF to LF and remove stray CR
    let query = query.replace("\r\n", "\n").replace('\r', "\n");
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
                // Parse WITH ... [ORDER BY ...] [SKIP n] [LIMIT n] [RETURN ...] or [DETACH DELETE ...]
                let mut body = t[5..].trim();
                let mut distinct = false;
                let bu = body.to_uppercase();
                if bu.starts_with("DISTINCT ") {
                    distinct = true;
                    body = body[9..].trim();
                }
                // Check for trailing RETURN or DELETE clauses
                let mut trailing_return: Option<&str> = None;
                let mut trailing_delete: Option<&str> = None;
                let upb = body.to_uppercase();
                if let Some(i) = find_keyword_boundary(&upb, "DETACH DELETE") {
                    trailing_delete = Some(&body[i..]);
                    body = body[..i].trim();
                } else if let Some(i) = find_keyword_boundary(&upb, "DELETE") {
                    trailing_delete = Some(&body[i..]);
                    body = body[..i].trim();
                } else if let Some(i) = find_keyword_boundary(&upb, "RETURN") {
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
                // If there is a trailing DELETE, parse it
                if let Some(del) = trailing_delete {
                    let del_up = del.to_uppercase();
                    if del_up.starts_with("DETACH DELETE ") {
                        let vars_str = &del[14..];
                        let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
                        clauses.push(Clause::Delete { vars, detach: true });
                    } else if del_up.starts_with("DELETE ") {
                        let vars_str = &del[7..];
                        let vars = split_top_level_comma(vars_str).into_iter().map(|s| s.trim().to_string()).collect();
                        clauses.push(Clause::Delete { vars, detach: false });
                    }
                }
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
        // Support multiple CREATE clauses: CREATE (a) CREATE (a)-[:R]->(b) ...
        // Split on CREATE keyword boundaries while preserving patterns
        let body = &q[6..].trim();
        
        // Check for RETURN at the end
        let body_up = body.to_uppercase();
        let (creates_part, return_part) = if let Some(ret_idx) = find_keyword_boundary(&body_up, "RETURN") {
            (&body[..ret_idx], Some(&body[ret_idx..]))
        } else {
            (*body, None)
        };
        
        // Split by CREATE keyword to handle multiple CREATE clauses
        let mut remaining = creates_part.to_string();
        loop {
            let rem_up = remaining.to_uppercase();
            // Find next CREATE keyword (not at position 0)
            if let Some(next_create) = find_keyword_boundary(&rem_up[1..], "CREATE") {
                let next_idx = next_create + 1; // offset by 1 since we searched from position 1
                let first_part = remaining[..next_idx].trim();
                if !first_part.is_empty() {
                    let mut patterns = Vec::new();
                    for pat in split_top_level_comma(first_part) { 
                        if !pat.is_empty() { patterns.push(parse_pattern(&pat)?); } 
                    }
                    if !patterns.is_empty() {
                        clauses.push(Clause::Create { patterns });
                    }
                }
                // Skip "CREATE" keyword for next iteration
                remaining = remaining[next_idx + 6..].trim().to_string();
            } else {
                // No more CREATE keywords, process remaining
                if !remaining.is_empty() {
                    let mut patterns = Vec::new();
                    for pat in split_top_level_comma(&remaining) { 
                        if !pat.is_empty() { patterns.push(parse_pattern(&pat)?); } 
                    }
                    if !patterns.is_empty() {
                        clauses.push(Clause::Create { patterns });
                    }
                }
                break;
            }
        }
        
        // Handle RETURN clause if present
        if let Some(ret) = return_part {
            let ret_body = ret.strip_prefix("RETURN").or_else(|| ret.strip_prefix("return")).unwrap_or(ret).trim();
            let mut limit: Option<usize> = None;
            let mut skip: Option<usize> = None;
            let mut working = ret_body.to_string();
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

#[derive(Clone)]
enum Val { NodeId(Uuid), RelId(Uuid) }

/// Evaluate an expression to a string value given a row context
fn eval_expr_to_string(
    expr: &Expr,
    row: &HashMap<String, Val>,
    db: &GraphDatabase,
    _params: &HashMap<String, String>,
) -> Option<String>
where Val: Clone {
    match expr {
        Expr::Var(v) => {
            if let Some(Val::NodeId(id)) = row.get(v) { Some(id.to_string()) }
            else if let Some(Val::RelId(id)) = row.get(v) { Some(id.to_string()) }
            else { None }
        }
        Expr::Prop(inner, prop) => {
            if let Expr::Var(v) = &**inner {
                if let Some(Val::NodeId(id)) = row.get(v) {
                    db.get_node(*id).and_then(|n| n.metadata.get(prop).cloned())
                } else if let Some(Val::RelId(id)) = row.get(v) {
                    db.get_relationship(*id).and_then(|r| r.metadata.get(prop).cloned())
                } else { None }
            } else { None }
        }
        Expr::Str(s) => Some(s.clone()),
        Expr::Num(n) => Some(n.to_string()),
        Expr::Bool(b) => Some(b.to_string()),
        Expr::Null => None,
        Expr::FuncId(v) => {
            if let Some(Val::NodeId(id)) = row.get(v) { Some(id.to_string()) }
            else if let Some(Val::RelId(id)) = row.get(v) { Some(id.to_string()) }
            else { None }
        }
        Expr::FuncTimestamp(v) => {
            if let Some(Val::NodeId(id)) = row.get(v) {
                let t = id.get_timestamp().unwrap().to_unix();
                Some(((t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000).to_string())
            } else if let Some(Val::RelId(id)) = row.get(v) {
                let t = id.get_timestamp().unwrap().to_unix();
                Some(((t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000).to_string())
            } else { None }
        }
        // String functions
        Expr::FuncToUpper(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| s.to_uppercase())
        }
        Expr::FuncToLower(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| s.to_lowercase())
        }
        Expr::FuncTrim(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| s.trim().to_string())
        }
        Expr::FuncLTrim(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| s.trim_start().to_string())
        }
        Expr::FuncRTrim(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| s.trim_end().to_string())
        }
        Expr::FuncReverse(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| s.chars().rev().collect())
        }
        Expr::FuncSize(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| s.len().to_string())
        }
        Expr::FuncReplace(s, search, repl) => {
            let sv = eval_expr_to_string(s, row, db, _params)?;
            let search_v = eval_expr_to_string(search, row, db, _params)?;
            let repl_v = eval_expr_to_string(repl, row, db, _params)?;
            Some(sv.replace(&search_v, &repl_v))
        }
        Expr::FuncSubstring(s, start, len_opt) => {
            let sv = eval_expr_to_string(s, row, db, _params)?;
            let start_v: usize = eval_expr_to_string(start, row, db, _params)?.parse().ok()?;
            let chars: Vec<char> = sv.chars().collect();
            if start_v >= chars.len() { return Some(String::new()); }
            let end = if let Some(len) = len_opt {
                let len_v: usize = eval_expr_to_string(len, row, db, _params)?.parse().ok()?;
                (start_v + len_v).min(chars.len())
            } else { chars.len() };
            Some(chars[start_v..end].iter().collect())
        }
        Expr::FuncLeft(s, n) => {
            let sv = eval_expr_to_string(s, row, db, _params)?;
            let nv: usize = eval_expr_to_string(n, row, db, _params)?.parse().ok()?;
            Some(sv.chars().take(nv).collect())
        }
        Expr::FuncRight(s, n) => {
            let sv = eval_expr_to_string(s, row, db, _params)?;
            let nv: usize = eval_expr_to_string(n, row, db, _params)?.parse().ok()?;
            let chars: Vec<char> = sv.chars().collect();
            let start = chars.len().saturating_sub(nv);
            Some(chars[start..].iter().collect())
        }
        // Type conversion
        Expr::FuncToString(inner) => eval_expr_to_string(inner, row, db, _params),
        Expr::FuncToInteger(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| (f as i64).to_string())
        }
        Expr::FuncToFloat(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.to_string())
        }
        Expr::FuncToBoolean(inner) => {
            eval_expr_to_string(inner, row, db, _params).map(|s| {
                match s.to_lowercase().as_str() {
                    "true" | "1" => "true".to_string(),
                    "false" | "0" | "" => "false".to_string(),
                    _ => "false".to_string(),
                }
            })
        }
        // Math functions
        Expr::FuncAbs(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.abs().to_string())
        }
        Expr::FuncCeil(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.ceil().to_string())
        }
        Expr::FuncFloor(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.floor().to_string())
        }
        Expr::FuncRound(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.round().to_string())
        }
        Expr::FuncSign(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| if f > 0.0 { "1" } else if f < 0.0 { "-1" } else { "0" }.to_string())
        }
        Expr::FuncSqrt(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.sqrt().to_string())
        }
        Expr::FuncLog(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.ln().to_string())
        }
        Expr::FuncLog10(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.log10().to_string())
        }
        Expr::FuncExp(inner) => {
            eval_expr_to_string(inner, row, db, _params)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.exp().to_string())
        }
        Expr::FuncPow(base, exp) => {
            let b = eval_expr_to_string(base, row, db, _params)?.parse::<f64>().ok()?;
            let e = eval_expr_to_string(exp, row, db, _params)?.parse::<f64>().ok()?;
            Some(b.powf(e).to_string())
        }
        Expr::FuncRand => {
            // Simple pseudo-random using system time
            use std::time::{SystemTime, UNIX_EPOCH};
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
            Some(((nanos as f64) / 1_000_000_000.0).to_string())
        }
        // Node/Relationship introspection
        Expr::FuncLabels(inner) => {
            if let Expr::Var(v) = &**inner {
                if let Some(Val::NodeId(id)) = row.get(v) {
                    db.get_node(*id).map(|n| format!("[\"{}\"]", n.label))
                } else { None }
            } else { None }
        }
        Expr::FuncType(inner) => {
            if let Expr::Var(v) = &**inner {
                if let Some(Val::RelId(id)) = row.get(v) {
                    db.get_relationship(*id).map(|r| r.label.clone())
                } else { None }
            } else { None }
        }
        Expr::FuncKeys(inner) => {
            if let Expr::Var(v) = &**inner {
                if let Some(Val::NodeId(id)) = row.get(v) {
                    db.get_node(*id).map(|n| {
                        let keys: Vec<String> = n.metadata.keys().map(|k| format!("\"{}\"", k)).collect();
                        format!("[{}]", keys.join(", "))
                    })
                } else if let Some(Val::RelId(id)) = row.get(v) {
                    db.get_relationship(*id).map(|r| {
                        let keys: Vec<String> = r.metadata.keys().map(|k| format!("\"{}\"", k)).collect();
                        format!("[{}]", keys.join(", "))
                    })
                } else { None }
            } else { None }
        }
        // Coalesce - returns first non-null value
        Expr::FuncCoalesce(exprs) => {
            for e in exprs {
                if let Some(v) = eval_expr_to_string(e, row, db, _params) {
                    return Some(v);
                }
            }
            None
        }
        // Alias - evaluate inner
        Expr::Alias(inner, _) => eval_expr_to_string(inner, row, db, _params),
        _ => None,
    }
}

/// Split query by UNION / UNION ALL at top level
fn split_union(query: &str) -> Vec<(String, bool)> {
    // Returns Vec of (subquery, is_union_all)
    // is_union_all indicates whether this part was preceded by UNION ALL
    let mut parts: Vec<(String, bool)> = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    let bytes = query.as_bytes();
    let n = bytes.len();
    let mut in_sq = false;
    let mut in_dq = false;
    let mut paren_depth = 0;
    let mut next_is_union_all = false;
    
    while i < n {
        let c = bytes[i] as char;
        if c == '\'' && !in_dq { in_sq = !in_sq; current.push(c); i += 1; continue; }
        if c == '"' && !in_sq { in_dq = !in_dq; current.push(c); i += 1; continue; }
        if !in_sq && !in_dq {
            if c == '(' { paren_depth += 1; }
            else if c == ')' { paren_depth -= 1; }
            
            // Check for UNION ALL first (longer match)
            if paren_depth == 0 && i + 9 <= n {
                let seg_up = query[i..].to_uppercase();
                if seg_up.starts_with("UNION ALL") {
                    let prev_ws = i == 0 || bytes[i-1].is_ascii_whitespace();
                    // Check character after "UNION ALL" (9 chars)
                    let next_ws = i + 9 >= n || bytes[i+9].is_ascii_whitespace();
                    if prev_ws && next_ws {
                        let trimmed = current.trim().to_string();
                        if !trimmed.is_empty() {
                            parts.push((trimmed, next_is_union_all));
                        }
                        current.clear();
                        next_is_union_all = true;
                        i += 9;
                        continue;
                    }
                }
            }
            // Check for plain UNION (not followed by ALL)
            if paren_depth == 0 && i + 5 <= n {
                let seg_up = query[i..i+5].to_uppercase();
                if seg_up == "UNION" {
                    let prev_ws = i == 0 || bytes[i-1].is_ascii_whitespace();
                    let next_ws = i + 5 >= n || bytes[i+5].is_ascii_whitespace();
                    // Make sure it's not UNION ALL
                    let followed_by_all = if i + 9 <= n {
                        let after = query[i+5..].trim_start().to_uppercase();
                        after.starts_with("ALL") && (after.len() == 3 || after.chars().nth(3).map(|c| c.is_whitespace()).unwrap_or(true))
                    } else { false };
                    if prev_ws && next_ws && !followed_by_all {
                        let trimmed = current.trim().to_string();
                        if !trimmed.is_empty() {
                            parts.push((trimmed, next_is_union_all));
                        }
                        current.clear();
                        next_is_union_all = false;
                        i += 5;
                        continue;
                    }
                }
            }
        }
        current.push(c);
        i += 1;
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        parts.push((trimmed, next_is_union_all));
    }
    parts
}

pub fn execute_cypher_with_params(db: &mut GraphDatabase, query: &str, params: &HashMap<String, String>) -> Result<Vec<QueryResultRow>> {
    // Normalize line endings: convert CRLF to LF and remove stray CR
    let query = query.replace("\r\n", "\n").replace('\r', "\n");
    let query = query.as_str();
    
    // Handle UNION / UNION ALL
    let union_parts = split_union(query);
    if union_parts.len() > 1 {
        let mut all_results: Vec<QueryResultRow> = Vec::new();
        let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
        
        // Check if any part uses UNION ALL - if so, the whole query keeps duplicates
        let any_union_all = union_parts.iter().any(|(_, is_all)| *is_all);
        
        for (subquery, _) in union_parts.iter() {
            if subquery.is_empty() { continue; }
            let sub_results = execute_cypher_with_params(db, subquery, params)?;
            
            for row in sub_results {
                if any_union_all {
                    // UNION ALL: keep all rows including duplicates
                    all_results.push(row);
                } else {
                    // Plain UNION: deduplicate by creating a key from the row
                    let key = match &row {
                        QueryResultRow::Node { id, .. } => format!("N:{}", id),
                        QueryResultRow::Relationship { id, .. } => format!("R:{}", id),
                        QueryResultRow::Info(s) => format!("I:{}", s),
                    };
                    if seen_keys.insert(key) {
                        all_results.push(row);
                    }
                }
            }
        }
        return Ok(all_results);
    }
    
    let clauses = parse(query)?;
    // binding map: var -> either Node or Relationship id (uses module-level Val enum)
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
                // WHERE support: conjunctive/disjunctive clauses with AND/OR; supports
                // - id(a) <op> id(b)
                // - var.prop <op> literal
                // - var.prop CONTAINS 'substr'
                // - var.prop IS NULL / IS NOT NULL
                // - var.prop IN [list]
                // - NOT condition
                // - condition OR condition
                
                /// Split by a keyword (AND or OR) respecting quotes and parentheses
                fn split_by_keyword(s: &str, keyword: &str) -> Vec<String> {
                    let mut out = Vec::new();
                    let mut start = 0usize;
                    let mut i = 0usize;
                    let bytes = s.as_bytes();
                    let n = bytes.len();
                    let kw_len = keyword.len();
                    let mut in_sq = false;
                    let mut in_dq = false;
                    let mut paren_depth = 0i32;
                    let mut bracket_depth = 0i32;
                    
                    while i < n {
                        let c = bytes[i] as char;
                        if c == '\'' && !in_dq { in_sq = !in_sq; i += 1; continue; }
                        if c == '"' && !in_sq { in_dq = !in_dq; i += 1; continue; }
                        if !in_sq && !in_dq {
                            if c == '(' { paren_depth += 1; }
                            else if c == ')' { paren_depth -= 1; }
                            else if c == '[' { bracket_depth += 1; }
                            else if c == ']' { bracket_depth -= 1; }
                            
                            // Only split at top level (not inside parens or brackets)
                            if paren_depth == 0 && bracket_depth == 0 && i + kw_len <= n {
                                let seg = &s[i..i+kw_len];
                                if seg.eq_ignore_ascii_case(keyword) {
                                    let prev_ws = i == 0 || bytes[i-1].is_ascii_whitespace();
                                    let next_ws = i+kw_len >= n || bytes[i+kw_len].is_ascii_whitespace();
                                    if prev_ws && next_ws {
                                        out.push(s[start..i].trim().to_string());
                                        start = i + kw_len;
                                        i += kw_len;
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
                
                fn split_where_and(s: &str) -> Vec<String> { split_by_keyword(s, "AND") }
                fn split_where_or(s: &str) -> Vec<String> { split_by_keyword(s, "OR") }

                fn trim_quotes_owned(s: &str) -> String { trim_quotes(s) }
                
                /// Parse IS NULL check: var.prop IS NULL or var IS NULL
                fn parse_is_null(expr: &str) -> Option<(String, Option<String>)> {
                    let up = expr.to_uppercase();
                    if let Some(i) = up.find(" IS NULL") {
                        // Make sure it's not IS NOT NULL
                        if up.contains(" IS NOT NULL") { return None; }
                        let lhs = expr[..i].trim();
                        if let Some(dot) = lhs.find('.') {
                            let var = lhs[..dot].trim().to_string();
                            let prop = lhs[dot+1..].trim().to_string();
                            return Some((var, Some(prop)));
                        } else {
                            return Some((lhs.to_string(), None));
                        }
                    }
                    None
                }
                
                /// Parse IS NOT NULL check
                fn parse_is_not_null(expr: &str) -> Option<(String, Option<String>)> {
                    let up = expr.to_uppercase();
                    if let Some(i) = up.find(" IS NOT NULL") {
                        let lhs = expr[..i].trim();
                        if let Some(dot) = lhs.find('.') {
                            let var = lhs[..dot].trim().to_string();
                            let prop = lhs[dot+1..].trim().to_string();
                            return Some((var, Some(prop)));
                        } else {
                            return Some((lhs.to_string(), None));
                        }
                    }
                    None
                }
                
                /// Parse IN operator: var.prop IN [list] or var IN [list]
                fn parse_in_list(expr: &str) -> Option<(String, Option<String>, Vec<String>)> {
                    let up = expr.to_uppercase();
                    if let Some(i) = up.find(" IN ") {
                        let lhs = expr[..i].trim();
                        let rhs = expr[i+4..].trim();
                        // rhs should be a list [a, b, c] or a parameter $param
                        if rhs.starts_with('[') && rhs.ends_with(']') {
                            let inner = &rhs[1..rhs.len()-1];
                            let items: Vec<String> = split_func_args(inner)
                                .into_iter()
                                .map(|s| trim_quotes(&s))
                                .collect();
                            if let Some(dot) = lhs.find('.') {
                                let var = lhs[..dot].trim().to_string();
                                let prop = lhs[dot+1..].trim().to_string();
                                return Some((var, Some(prop), items));
                            } else {
                                return Some((lhs.to_string(), None, items));
                            }
                        }
                    }
                    None
                }
                
                /// Parse regex match: var.prop =~ 'pattern'
                fn parse_regex_match(expr: &str) -> Option<(String, String, String)> {
                    if let Some(i) = expr.find("=~") {
                        let lhs = expr[..i].trim();
                        let rhs = expr[i+2..].trim();
                        if let Some(dot) = lhs.find('.') {
                            let var = lhs[..dot].trim().to_string();
                            let prop = lhs[dot+1..].trim().to_string();
                            return Some((var, prop, trim_quotes(rhs)));
                        }
                    }
                    None
                }

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

                // Parse timestamp(var) op value comparisons
                fn parse_timestamp_compare(expr: &str) -> Option<(String, String, String)> {
                    let up = expr.to_uppercase();
                    if !up.contains("TIMESTAMP(") { return None; }
                    let ops = ["<=", ">=", "<>", "<", ">", "="];
                    for op in ops {
                        if let Some(i) = expr.find(op) {
                            let lhs = expr[..i].trim();
                            let rhs = expr[i+op.len()..].trim();
                            let lhs_up = lhs.to_uppercase();
                            if lhs_up.starts_with("TIMESTAMP(") && lhs.ends_with(')') {
                                let var = lhs[10..lhs.len()-1].trim().to_string();
                                return Some((var, op.to_string(), rhs.to_string()));
                            }
                        }
                    }
                    None
                }

                /// Evaluate a single atomic condition against a row
                /// Returns Some(true/false) if evaluable, None if unknown/unsupported
                fn eval_atomic_condition(
                    c: &str,
                    row: &HashMap<String, Val>,
                    db: &GraphDatabase,
                    params: &HashMap<String, String>,
                ) -> Result<Option<bool>> {
                    let c = c.trim();
                    let cu = c.to_uppercase();
                    
                    // Handle NOT prefix
                    if cu.starts_with("NOT ") {
                        let inner = &c[4..].trim();
                        // Remove parentheses if present
                        let inner = if inner.starts_with('(') && inner.ends_with(')') {
                            &inner[1..inner.len()-1]
                        } else { inner };
                        return match eval_atomic_condition(inner, row, db, params)? {
                            Some(b) => Ok(Some(!b)),
                            None => Ok(None),
                        };
                    }
                    
                    // IS NOT NULL (check before IS NULL)
                    if let Some((var, prop_opt)) = parse_is_not_null(c) {
                        if let Some(prop) = prop_opt {
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                if let Some(n) = db.get_node(*id) {
                                    return Ok(Some(n.metadata.contains_key(&prop)));
                                }
                            } else if let Some(Val::RelId(id)) = row.get(&var) {
                                if let Some(r) = db.get_relationship(*id) {
                                    return Ok(Some(r.metadata.contains_key(&prop)));
                                }
                            }
                            return Ok(Some(false));
                        } else {
                            return Ok(Some(row.contains_key(&var)));
                        }
                    }
                    
                    // IS NULL
                    if let Some((var, prop_opt)) = parse_is_null(c) {
                        if let Some(prop) = prop_opt {
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                if let Some(n) = db.get_node(*id) {
                                    return Ok(Some(!n.metadata.contains_key(&prop)));
                                }
                            } else if let Some(Val::RelId(id)) = row.get(&var) {
                                if let Some(r) = db.get_relationship(*id) {
                                    return Ok(Some(!r.metadata.contains_key(&prop)));
                                }
                            }
                            return Ok(Some(true)); // var not found means null
                        } else {
                            return Ok(Some(!row.contains_key(&var)));
                        }
                    }
                    
                    // IN list
                    if let Some((var, prop_opt, items)) = parse_in_list(c) {
                        let val = if let Some(prop) = prop_opt {
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                db.get_node(*id).and_then(|n| n.metadata.get(&prop).cloned())
                            } else if let Some(Val::RelId(id)) = row.get(&var) {
                                db.get_relationship(*id).and_then(|r| r.metadata.get(&prop).cloned())
                            } else { None }
                        } else {
                            // var itself - get its string representation
                            if let Some(Val::NodeId(id)) = row.get(&var) {
                                Some(id.to_string())
                            } else if let Some(Val::RelId(id)) = row.get(&var) {
                                Some(id.to_string())
                            } else { None }
                        };
                        return Ok(Some(val.map(|v| items.contains(&v)).unwrap_or(false)));
                    }
                    
                    // Regex match =~
                    if let Some((var, prop, pattern)) = parse_regex_match(c) {
                        if let Some(Val::NodeId(id)) = row.get(&var) {
                            if let Some(n) = db.get_node(*id) {
                                let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                // Simple regex support using Rust's regex crate would be ideal,
                                // but for now do basic pattern matching
                                // Convert Cypher regex to simple contains/starts/ends check
                                let pass = if pattern.starts_with(".*") && pattern.ends_with(".*") {
                                    sv.contains(&pattern[2..pattern.len()-2])
                                } else if pattern.starts_with(".*") {
                                    sv.ends_with(&pattern[2..])
                                } else if pattern.ends_with(".*") {
                                    sv.starts_with(&pattern[..pattern.len()-2])
                                } else {
                                    sv == pattern
                                };
                                return Ok(Some(pass));
                            }
                        }
                        return Ok(Some(false));
                    }
                    
                    // id compare
                    if let Some((lv, op, rv)) = parse_id_compare(c) {
                        if let (Some(Val::NodeId(a)), Some(Val::NodeId(b))) = (row.get(&lv), row.get(&rv)) {
                            let la = a.as_u128(); let lb = b.as_u128();
                            let pass = match op.as_str() { "<"=>la<lb, "<="=>la<=lb, ">"=>la>lb, ">="=>la>=lb, "="=>la==lb, "<>"=>la!=lb, _=>true };
                            return Ok(Some(pass));
                        }
                        return Ok(Some(false));
                    }
                    
                    // CONTAINS
                    if let Some((var, prop, rhs)) = parse_contains(c) {
                        let val = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                        if let Some(Val::NodeId(id)) = row.get(&var) {
                            if let Some(n) = db.get_node(*id) {
                                let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                return Ok(Some(sv.contains(&val)));
                            }
                        } else if let Some(Val::RelId(id)) = row.get(&var) {
                            if let Some(r) = db.get_relationship(*id) {
                                let sv = r.metadata.get(&prop).cloned().unwrap_or_default();
                                return Ok(Some(sv.contains(&val)));
                            }
                        }
                        return Ok(Some(false));
                    }
                    
                    // STARTS WITH
                    if let Some((var, prop, rhs)) = parse_starts_with(c) {
                        let val = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                        if let Some(Val::NodeId(id)) = row.get(&var) {
                            if let Some(n) = db.get_node(*id) {
                                let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                return Ok(Some(sv.starts_with(&val)));
                            }
                        } else if let Some(Val::RelId(id)) = row.get(&var) {
                            if let Some(r) = db.get_relationship(*id) {
                                let sv = r.metadata.get(&prop).cloned().unwrap_or_default();
                                return Ok(Some(sv.starts_with(&val)));
                            }
                        }
                        return Ok(Some(false));
                    }
                    
                    // ENDS WITH
                    if let Some((var, prop, rhs)) = parse_ends_with(c) {
                        let val = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                        if let Some(Val::NodeId(id)) = row.get(&var) {
                            if let Some(n) = db.get_node(*id) {
                                let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                return Ok(Some(sv.ends_with(&val)));
                            }
                        } else if let Some(Val::RelId(id)) = row.get(&var) {
                            if let Some(r) = db.get_relationship(*id) {
                                let sv = r.metadata.get(&prop).cloned().unwrap_or_default();
                                return Ok(Some(sv.ends_with(&val)));
                            }
                        }
                        return Ok(Some(false));
                    }
                    
                    // timestamp(var) op value
                    if let Some((var, op, rhs)) = parse_timestamp_compare(c) {
                        let rhs_val: f64 = rhs.parse().unwrap_or(0.0);
                        let ts_val = if let Some(Val::NodeId(id)) = row.get(&var) {
                            let t = id.get_timestamp().unwrap().to_unix();
                            (t.0 as f64) * 1000.0 + (t.1 as f64) / 1_000_000.0
                        } else if let Some(Val::RelId(id)) = row.get(&var) {
                            let t = id.get_timestamp().unwrap().to_unix();
                            (t.0 as f64) * 1000.0 + (t.1 as f64) / 1_000_000.0
                        } else { return Ok(Some(false)); };
                        let pass = match op.as_str() {
                            "<" => ts_val < rhs_val,
                            "<=" => ts_val <= rhs_val,
                            ">" => ts_val > rhs_val,
                            ">=" => ts_val >= rhs_val,
                            "=" => (ts_val - rhs_val).abs() < 1.0,
                            "<>" => (ts_val - rhs_val).abs() >= 1.0,
                            _ => true,
                        };
                        return Ok(Some(pass));
                    }
                    
                    // var.prop op literal
                    if let Some((var, prop, op, rhs)) = parse_var_prop_comp(c) {
                        let lit = if rhs.starts_with('"') || rhs.starts_with('\'') { trim_quotes_owned(&rhs) } else { resolve_param(&rhs, params)? };
                        if let Some(Val::NodeId(id)) = row.get(&var) {
                            if let Some(n) = db.get_node(*id) {
                                let sv = n.metadata.get(&prop).cloned().unwrap_or_default();
                                let as_num = |s: &str| s.parse::<f64>().ok();
                                let pass = if let (Some(a), Some(b)) = (as_num(&sv), as_num(&lit)) {
                                    match op.as_str() { "<"=>a<b, "<="=>a<=b, ">"=>a>b, ">="=>a>=b, "="=> a==b, "<>"=> a!=b, _=>true }
                                } else {
                                    match op.as_str() { "="=> sv==lit, "<>"=> sv!=lit, "<"=> sv<lit, ">"=> sv>lit, "<="=> sv<=lit, ">="=> sv>=lit, _=> true }
                                };
                                return Ok(Some(pass));
                            }
                        } else if let Some(Val::RelId(id)) = row.get(&var) {
                            if let Some(r) = db.get_relationship(*id) {
                                let sv = r.metadata.get(&prop).cloned().unwrap_or_default();
                                let as_num = |s: &str| s.parse::<f64>().ok();
                                let pass = if let (Some(a), Some(b)) = (as_num(&sv), as_num(&lit)) {
                                    match op.as_str() { "<"=>a<b, "<="=>a<=b, ">"=>a>b, ">="=>a>=b, "="=> a==b, "<>"=> a!=b, _=>true }
                                } else {
                                    match op.as_str() { "="=> sv==lit, "<>"=> sv!=lit, "<"=> sv<lit, ">"=> sv>lit, "<="=> sv<=lit, ">="=> sv>=lit, _=> true }
                                };
                                return Ok(Some(pass));
                            }
                        }
                        return Ok(Some(false));
                    }
                    
                    // unsupported clause -> return None (unknown)
                    Ok(None)
                }
                
                /// Evaluate a WHERE expression with AND/OR support
                fn eval_where_expr(
                    expr: &str,
                    row: &HashMap<String, Val>,
                    db: &GraphDatabase,
                    params: &HashMap<String, String>,
                ) -> Result<bool> {
                    let expr = expr.trim();
                    
                    // Handle parenthesized expressions
                    if expr.starts_with('(') && expr.ends_with(')') {
                        // Check if the parens are balanced and wrap the whole expression
                        let inner = &expr[1..expr.len()-1];
                        let mut depth = 0;
                        let mut balanced = true;
                        for c in inner.chars() {
                            match c {
                                '(' => depth += 1,
                                ')' => { depth -= 1; if depth < 0 { balanced = false; break; } }
                                _ => {}
                            }
                        }
                        if balanced && depth == 0 {
                            return eval_where_expr(inner, row, db, params);
                        }
                    }
                    
                    // First split by OR (lower precedence)
                    let or_parts = split_where_or(expr);
                    if or_parts.len() > 1 {
                        // Any OR clause passing means true
                        for part in &or_parts {
                            if eval_where_expr(part, row, db, params)? {
                                return Ok(true);
                            }
                        }
                        return Ok(false);
                    }
                    
                    // Then split by AND (higher precedence)
                    let and_parts = split_where_and(expr);
                    if and_parts.len() > 1 {
                        // All AND clauses must pass
                        for part in &and_parts {
                            if !eval_where_expr(part, row, db, params)? {
                                return Ok(false);
                            }
                        }
                        return Ok(true);
                    }
                    
                    // Single atomic condition
                    match eval_atomic_condition(expr, row, db, params)? {
                        Some(b) => Ok(b),
                        None => Ok(true), // Unknown conditions pass (fail-safe)
                    }
                }
                
                let mut filtered: Vec<HashMap<String, Val>> = Vec::new();
                for row in &rows {
                    if eval_where_expr(&w, row, db, params)? {
                        filtered.push(row.clone());
                    }
                }
                rows = filtered;
            }
            Clause::With { items, distinct: _distinct, order_by, skip, limit } => {
                // Check if we have aggregation (collect) - requires grouping
                let has_collect = items.iter().any(|it| matches!(it, Expr::FuncCollect(_)) || 
                    matches!(it, Expr::Alias(inner, _) if matches!(&**inner, Expr::FuncCollect(_))));
                
                if has_collect {
                    // Aggregation mode: group by non-aggregate items, collect the rest
                    // Find grouping keys (aliases of properties or vars) and collect targets
                    let mut group_key_exprs: Vec<(Expr, String)> = Vec::new(); // (expr, alias)
                    let mut collect_exprs: Vec<(String, String)> = Vec::new(); // (var_to_collect, alias)
                    
                    for it in &items {
                        match it {
                            Expr::Alias(inner, alias) => {
                                match &**inner {
                                    Expr::FuncCollect(v) => collect_exprs.push((v.clone(), alias.clone())),
                                    _ => group_key_exprs.push((*inner.clone(), alias.clone())),
                                }
                            }
                            Expr::FuncCollect(v) => collect_exprs.push((v.clone(), format!("collect({})", v))),
                            Expr::Var(v) => group_key_exprs.push((Expr::Var(v.clone()), v.clone())),
                            _ => {}
                        }
                    }
                    
                    // Helper to evaluate an expression to a string key for grouping
                    let eval_to_string = |expr: &Expr, r: &HashMap<String, Val>, db: &GraphDatabase| -> String {
                        match expr {
                            Expr::Prop(inner, prop) => {
                                if let Expr::Var(v) = &**inner {
                                    if let Some(Val::NodeId(id)) = r.get(v) {
                                        if let Some(n) = db.get_node(*id) {
                                            return n.metadata.get(prop).cloned().unwrap_or_default();
                                        }
                                    }
                                }
                                String::new()
                            }
                            Expr::Var(v) => {
                                if let Some(Val::NodeId(id)) = r.get(v) { id.to_string() }
                                else if let Some(Val::RelId(id)) = r.get(v) { id.to_string() }
                                else { String::new() }
                            }
                            _ => String::new()
                        }
                    };
                    
                    // Group rows by the grouping key
                    use std::collections::BTreeMap;
                    let mut groups: BTreeMap<Vec<String>, Vec<HashMap<String, Val>>> = BTreeMap::new();
                    for r in &rows {
                        let key: Vec<String> = group_key_exprs.iter().map(|(e, _)| eval_to_string(e, r, db)).collect();
                        groups.entry(key).or_default().push(r.clone());
                    }
                    
                    // Build output rows: one per group
                    let mut new_rows: Vec<HashMap<String, Val>> = Vec::new();
                    for (_group_key, group_rows) in groups {
                        let mut proj: HashMap<String, Val> = HashMap::new();
                        
                        // Store group key values as special string vals (we need a way to pass them)
                        // For now, we'll store the first row's values for non-collect items
                        if let Some(first) = group_rows.first() {
                            for (expr, alias) in &group_key_exprs {
                                if let Expr::Var(v) = expr {
                                    if let Some(val) = first.get(v) {
                                        proj.insert(alias.clone(), val.clone());
                                    }
                                }
                            }
                        }
                        
                        // For collect: store all node IDs as a special list value
                        // We'll use a new Val variant or encode as comma-separated string
                        for (var_to_collect, alias) in &collect_exprs {
                            let collected: Vec<Uuid> = group_rows.iter()
                                .filter_map(|r| r.get(var_to_collect))
                                .filter_map(|v| match v {
                                    Val::NodeId(id) => Some(*id),
                                    Val::RelId(id) => Some(*id),
                                })
                                .collect();
                            // Store as first element for now (we need list support)
                            // For deduplication, we want all but the first
                            if collected.len() > 1 {
                                // Store duplicates (all except first) for deletion
                                for id in collected.iter().skip(1) {
                                    let mut dup_row = proj.clone();
                                    dup_row.insert(alias.clone(), Val::NodeId(*id));
                                    new_rows.push(dup_row);
                                }
                            }
                        }
                    }
                    rows = new_rows;
                } else {
                    // Non-aggregation mode: project and sort as before
                    let mut keyed_rows: Vec<(Vec<String>, HashMap<String, Val>)> = Vec::new();
                    for r in &rows {
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
                                    Expr::FuncTimestamp(v) => {
                                        if let Some(Val::NodeId(id)) = r.get(v) {
                                            let ts = { let t = id.get_timestamp().unwrap().to_unix(); (t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000 };
                                            key_vals.push(ts.to_string());
                                        } else if let Some(Val::RelId(id)) = r.get(v) {
                                            let ts = { let t = id.get_timestamp().unwrap().to_unix(); (t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000 };
                                            key_vals.push(ts.to_string());
                                        } else { key_vals.push(String::new()); }
                                    }
                                    Expr::Str(s) => key_vals.push(s.clone()),
                                    _ => key_vals.push(String::new()),
                                }
                            }
                        }
                        let mut proj: HashMap<String, Val> = HashMap::new();
                        for it in &items {
                            match it {
                                Expr::Var(v) => {
                                    if let Some(val) = r.get(v) { proj.insert(v.clone(), val.clone()); }
                                }
                                Expr::Alias(inner, alias) => {
                                    if let Expr::Var(v) = &**inner {
                                        if let Some(val) = r.get(v) { proj.insert(alias.clone(), val.clone()); }
                                    }
                                }
                                _ => {}
                            }
                        }
                        keyed_rows.push((key_vals, proj));
                    }
                    if !order_by.is_empty() {
                        keyed_rows.sort_by(|a, b| {
                            let ka = &a.0; let kb = &b.0;
                            let mut ord = std::cmp::Ordering::Equal;
                            let len = ka.len().min(kb.len()).min(order_by.len());
                            for i in 0..len {
                                let asc = order_by[i].1;
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
                            Expr::FuncTimestamp(v) => {
                                if let Some(Val::NodeId(id)) = r.get(v) {
                                    let ts = { let t = id.get_timestamp().unwrap().to_unix(); (t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000 };
                                    out_rows.push(QueryResultRow::Info(ts.to_string()));
                                } else if let Some(Val::RelId(id)) = r.get(v) {
                                    let ts = { let t = id.get_timestamp().unwrap().to_unix(); (t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000 };
                                    out_rows.push(QueryResultRow::Info(ts.to_string()));
                                }
                            }
                            Expr::Str(s) => out_rows.push(QueryResultRow::Info(s.clone())),
                            Expr::Num(n) => out_rows.push(QueryResultRow::Info(n.to_string())),
                            Expr::Bool(b) => out_rows.push(QueryResultRow::Info(b.to_string())),
                            // Handle all other expression types via eval_expr_to_string
                            other => {
                                if let Some(val) = eval_expr_to_string(other, r, db, params) {
                                    out_rows.push(QueryResultRow::Info(val));
                                }
                            }
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
                                Expr::FuncTimestamp(v) => {
                                    if let Some(Val::NodeId(id)) = r.get(v) {
                                        let ts = { let t = id.get_timestamp().unwrap().to_unix(); (t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000 };
                                        key_vals.push(ts.to_string());
                                    } else if let Some(Val::RelId(id)) = r.get(v) {
                                        let ts = { let t = id.get_timestamp().unwrap().to_unix(); (t.0 as u64) * 1000 + (t.1 as u64) / 1_000_000 };
                                        key_vals.push(ts.to_string());
                                    } else { key_vals.push(String::new()); }
                                }
                                Expr::Str(s) => key_vals.push(s.clone()),
                                _ => key_vals.push(String::new()),
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
            Clause::Unwind { expr, var } => {
                // UNWIND expr AS var - expand list into rows
                // Support: UNWIND nodes[1..] AS toDelete or UNWIND nodes AS x
                let mut new_rows: Vec<HashMap<String, Val>> = Vec::new();
                for row in &rows {
                    // Parse the expression to find the list variable and optional slice
                    let expr_trimmed = expr.trim();
                    let (list_var, slice_start, slice_end) = if let Some(bracket_idx) = expr_trimmed.find('[') {
                        if expr_trimmed.ends_with(']') {
                            let var_name = expr_trimmed[..bracket_idx].trim();
                            let slice_part = &expr_trimmed[bracket_idx+1..expr_trimmed.len()-1];
                            if slice_part.contains("..") {
                                let parts: Vec<&str> = slice_part.split("..").collect();
                                let start = if parts[0].trim().is_empty() { None } else { parts[0].trim().parse::<usize>().ok() };
                                let end = if parts.len() < 2 || parts[1].trim().is_empty() { None } else { parts[1].trim().parse::<usize>().ok() };
                                (var_name, start, end)
                            } else {
                                // Single index like nodes[0]
                                let idx = slice_part.trim().parse::<usize>().ok();
                                (var_name, idx, idx.map(|i| i + 1))
                            }
                        } else {
                            (expr_trimmed, None, None)
                        }
                    } else {
                        (expr_trimmed, None, None)
                    };
                    
                    // Get the list from the row - we need to handle collected lists
                    // For now, if the variable exists as a single value, treat it as a one-element list
                    // The real list support would require a Val::List variant
                    if let Some(val) = row.get(list_var) {
                        // Single value case - just bind it if slice allows
                        let start = slice_start.unwrap_or(0);
                        let end = slice_end.unwrap_or(1);
                        if start == 0 && end >= 1 {
                            let mut new_row = row.clone();
                            new_row.insert(var.clone(), val.clone());
                            new_rows.push(new_row);
                        }
                        // If slice is [1..], skip the single element (empty result)
                    }
                }
                rows = new_rows;
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
