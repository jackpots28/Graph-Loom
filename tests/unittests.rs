

use graph_loom::gql::query_interface::{execute_query, execute_query_with_params, QueryOutcome, QueryResultRow};
use graph_loom::graph_utils::graph::GraphDatabase;
use uuid::Uuid;

fn new_db() -> GraphDatabase {
    GraphDatabase::new()
}

fn ids_from_rows(rows: &[QueryResultRow]) -> Vec<Uuid> {
    let mut out = Vec::new();
    for r in rows {
        match r {
            QueryResultRow::Node { id, .. } => out.push(*id),
            QueryResultRow::Relationship { id, .. } => out.push(*id),
            QueryResultRow::Info(_) => {}
        }
    }
    out
}

#[test]
fn cypher_params_in_pattern_and_where() {
    let mut db = new_db();
    // Seed data
    execute_query(&mut db, "CREATE (:Person {name: 'Neo'});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'The Matrix', released: 1999});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'The Matrix Reloaded', released: 2003});").unwrap();

    // MATCH with parameter in node pattern
    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), "Neo".to_string());
    let rows = execute_query_with_params(&mut db, "MATCH (p:Person {name: $name}) RETURN p", &params).unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0] {
        QueryResultRow::Node { label, metadata, .. } => {
            assert_eq!(label, "Person");
            assert_eq!(metadata.get("name").map(String::as_str), Some("Neo"));
        }
        _ => panic!("expected node row"),
    }

    // WHERE with numeric compare and CONTAINS using parameters
    params.clear();
    params.insert("year".to_string(), "2000".to_string());
    params.insert("substr".to_string(), "Matrix".to_string());
    let q = "
        MATCH (m:Movie)
        WHERE m.released > $year AND m.title CONTAINS $substr
        RETURN m.title
    ";
    let out = execute_query_with_params(&mut db, q, &params).unwrap();
    assert_eq!(out.rows.len(), 1);
    match &out.rows[0] {
        QueryResultRow::Info(s) => assert_eq!(s, "The Matrix Reloaded"),
        _ => panic!("expected title info"),
    }
}

#[test]
fn cypher_match_dot_label_and_where_property() {
    let mut db = new_db();
    // Seed: a Keyword node with property keyword = "theory" and another different one
    execute_query(&mut db, "CREATE (:Keyword {keyword: 'theory'})").unwrap();
    execute_query(&mut db, "CREATE (:Keyword {keyword: 'practice'})").unwrap();

    // Use the shorthand (n.Label) form together with WHERE on a property
    let q = r#"
        MATCH (n.Keyword)
        WHERE n.keyword = "theory"
        RETURN n
    "#;
    let out = execute_query(&mut db, q).unwrap();
    // Expect exactly one node back with the correct property
    let ids = ids_from_rows(&out.rows);
    assert_eq!(ids.len(), 1, "expected 1 matching Keyword node");
}

#[test]
fn graphdb_add_node_relationship_and_cascade_delete() {
    let mut db = new_db();
    let a = db.add_node("Person".to_string(), Default::default());
    let b = db.add_node("Company".to_string(), Default::default());
    let rid = db
        .add_relationship(a, b, "WORKS_AT".to_string(), Default::default())
        .expect("relationship should be created");

    assert!(db.get_relationship(rid).is_some());
    assert!(db.remove_node(a), "should remove node a");
    // Relationship should be removed too
    assert!(db.get_relationship(rid).is_none());
}

#[test]
fn graphdb_metadata_upsert_and_remove() {
    let mut db = new_db();
    let n = db.add_node("Person".to_string(), Default::default());
    assert!(db.upsert_node_metadata(n, "name".into(), "Ada".into()));
    assert!(db.upsert_node_metadata(n, "role".into(), "Engineer".into()));
    // Remove a key
    assert!(db.remove_node_metadata_key(n, "role"));
    // Removing non-existent returns false
    assert!(!db.remove_node_metadata_key(n, "role"));
}

#[test]
fn graphdb_add_relationship_missing_endpoints_fails() {
    let mut db = new_db();
    let u1 = Uuid::now_v7();
    let u2 = Uuid::now_v7();
    assert!(db
        .add_relationship(u1, u2, "KNOWS".to_string(), Default::default())
        .is_none());
}

#[test]
fn gql_create_match_where_delete_node_flow() {
    let mut db = new_db();
    // Create 2 persons
    let out = execute_query(
        &mut db,
        r#"
        CREATE NODE Person {name:"Ada", role:"Engineer"};
        CREATE NODE Person {name:"Bob", role:"Designer"};
        "#,
    )
    .expect("create ok");
    assert!(out.mutated);
    assert_eq!(out.affected_nodes, 2);
    // Capture Ada and Bob ids from creation output order
    let (ada_id, bob_id) = match (&out.rows[0], &out.rows[1]) {
        (QueryResultRow::Node { id: a, .. }, QueryResultRow::Node { id: b, .. }) => (*a, *b),
        _ => panic!("unexpected rows from CREATE"),
    };

    // Match all Person
    let m1 = execute_query(&mut db, "MATCH NODE Person;").expect("match ok");
    assert_eq!(m1.rows.len(), 2);

    // Match with property exact
    let m2 = execute_query(&mut db, "MATCH NODE Person {role:\"Engineer\"};").unwrap();
    assert_eq!(m2.rows.len(), 1);

    // WHERE by id, has(key), eq and ne (targeting Ada)
    let q = format!(
        "MATCH NODE Person WHERE id={} AND HAS(name) AND name=\"Ada\" AND role!=\"Manager\";",
        ada_id
    );
    let m3 = execute_query(&mut db, &q).unwrap();
    assert_eq!(m3.rows.len(), 1);

    // Delete Bob by id
    let dq = format!("DELETE NODE {};", bob_id);
    let d = execute_query(&mut db, &dq).unwrap();
    assert_eq!(d.affected_nodes, 1);

    let m4 = execute_query(&mut db, "MATCH NODE Person;").unwrap();
    assert_eq!(m4.rows.len(), 1);
}

#[test]
fn gql_create_rel_match_where_delete_flow() {
    let mut db = new_db();
    // create two nodes and a relationship
    let out = execute_query(
        &mut db,
        r#"
        CREATE NODE Person {name:"Ada"};
        CREATE NODE Company {name:"Acme"};
        "#,
    )
    .unwrap();
    let mut node_ids = Vec::new();
    for r in out.rows {
        if let QueryResultRow::Node { id, .. } = r { node_ids.push(id); }
    }
    assert_eq!(node_ids.len(), 2);
    let ada = node_ids[0];
    let acme = node_ids[1];

    let c = execute_query(
        &mut db,
        &format!(
            "CREATE REL from={} to={} label=WORKS_AT {{since:\"2021\"}};",
            ada, acme
        ),
    )
    .unwrap();
    assert_eq!(c.affected_relationships, 1);

    // Match rel by label
    let mr = execute_query(&mut db, "MATCH REL WORKS_AT;").unwrap();
    assert_eq!(mr.rows.len(), 1);

    // WHERE by from/to
    let w = execute_query(
        &mut db,
        &format!("MATCH REL WORKS_AT WHERE from={} AND to={};", ada, acme),
    )
    .unwrap();
    assert_eq!(w.rows.len(), 1);

    // Extract rel id and delete
    let rel_id = match &mr.rows[0] { QueryResultRow::Relationship { id, .. } => *id, _ => panic!("expected rel") };
    let dr = execute_query(&mut db, &format!("DELETE REL {};", rel_id)).unwrap();
    assert_eq!(dr.affected_relationships, 1);
}

#[test]
fn gql_errors_and_edge_cases() {
    let mut db = new_db();
    // Unrecognized statement
    assert!(execute_query(&mut db, "UPDATE NODE Person SET name=\"X\";").is_err());

    // Invalid UUID in WHERE
    assert!(execute_query(&mut db, "MATCH NODE Person WHERE id=not-a-uuid;").is_err());

    // CREATE REL with missing endpoints
    let bad = format!(
        "CREATE REL from={} to={} label=LIKES;",
        Uuid::now_v7(),
        Uuid::now_v7()
    );
    let err = execute_query(&mut db, &bad);
    assert!(err.is_err());
}

#[test]
fn gql_multi_statement_execution_aggregates_counts() {
    let mut db = new_db();
    let out: QueryOutcome = execute_query(
        &mut db,
        r#"
        CREATE NODE A {k:"v"};
        CREATE NODE B {x:"y"};
        CREATE NODE B {x:"z"};
        "#,
    )
    .unwrap();
    assert!(out.mutated);
    assert_eq!(out.affected_nodes, 3);

    let m = execute_query(&mut db, "MATCH NODE B {x:\"y\"};").unwrap();
    assert_eq!(m.rows.len(), 1);
}

#[test]
fn cypher_match_merge_pairwise_creation() {
    let mut db = new_db();
    // Create 3 nodes with same label 'asdf'
    let out = execute_query(
        &mut db,
        r#"
        CREATE NODE asdf {name:"n1"};
        CREATE NODE asdf {name:"n2"};
        CREATE NODE asdf {name:"n3"};
        "#,
    )
    .unwrap();
    assert_eq!(db.node_count(), 3);

    // Use Cypher-style statement to connect every unordered pair with RELATED_TO
    let q = "MATCH (a:asdf), (b:asdf) WHERE id(a) < id(b) MERGE (a)-[:RELATED_TO]->(b);";
    let r1 = execute_query(&mut db, q).unwrap();
    // For 3 nodes, unordered pairs = 3
    assert_eq!(r1.affected_relationships, 3);
    assert_eq!(db.relationship_count(), 3);

    // Running again should be idempotent (MERGE semantics) — no new relationships
    let r2 = execute_query(&mut db, q).unwrap();
    assert_eq!(r2.affected_relationships, 0);
    assert_eq!(db.relationship_count(), 3);
}

#[test]
fn cypher_delete_relationships_by_label() {
    let mut db = new_db();
    // Create 3 nodes with the same label 'asdf'
    execute_query(
        &mut db,
        r#"
        CREATE NODE asdf {name:"n1"};
        CREATE NODE asdf {name:"n2"};
        CREATE NODE asdf {name:"n3"};
        "#,
    )
    .unwrap();
    assert_eq!(db.node_count(), 3);

    // Connect every unordered pair with RELATED_TO
    let q = "MATCH (a:asdf), (b:asdf) WHERE id(a) < id(b) MERGE (a)-[:RELATED_TO]->(b);";
    execute_query(&mut db, q).unwrap();
    assert_eq!(db.relationship_count(), 3);

    // Now delete all RELATED_TO relationships using Cypher DELETE
    let del = "MATCH (a:asdf)-[r:RELATED_TO]-(b:asdf) DELETE r;";
    execute_query(&mut db, del).unwrap();
    assert_eq!(db.relationship_count(), 0);

    // Idempotent second delete
    execute_query(&mut db, del).unwrap();
    assert_eq!(db.relationship_count(), 0);
}

#[test]
fn cypher_match_node_with_props_and_return() {
    let mut db = new_db();
    // Create a Person Keanu Reeves
    execute_query(&mut db, "CREATE (:Person {name: 'Keanu Reeves'});").unwrap();
    let rows = execute_query(&mut db, "MATCH (p:Person {name: 'Keanu Reeves'}) RETURN p;").unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0] {
        QueryResultRow::Node { label, metadata, .. } => {
            assert_eq!(label, "Person");
            assert_eq!(metadata.get("name").map(String::as_str), Some("Keanu Reeves"));
        }
        _ => panic!("expected a node row"),
    }
}

#[test]
fn cypher_match_rel_with_props_and_return_property() {
    let mut db = new_db();
    // Create Tom Hanks and Forrest Gump, link with ACTED_IN role 'Forrest Gump'
    execute_query(&mut db, "CREATE (:Person {name: 'Tom Hanks'});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'Forrest Gump'});").unwrap();
    execute_query(&mut db, r#"
        MATCH (p:Person {name: 'Tom Hanks'}), (m:Movie {title: 'Forrest Gump'})
        CREATE (p)-[:ACTED_IN {role: 'Forrest Gump'}]->(m);
    "#).unwrap();

    let q = r#"
        MATCH (:Person {name: 'Tom Hanks'})-[r:ACTED_IN {role: 'Forrest Gump'}]->(m:Movie)
        RETURN m.title
    "#;
    let rows = execute_query(&mut db, q).unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0] { QueryResultRow::Info(s) => assert_eq!(s, "Forrest Gump"), _ => panic!("expected Info with title") }
}

#[test]
fn cypher_where_contains_and_numeric_compare() {
    let mut db = new_db();
    // Create some movies
    execute_query(&mut db, "CREATE (:Movie {title: 'The Matrix', released: 1999});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'The Matrix Reloaded', released: 2003});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'John Wick', released: 2014});").unwrap();

    let q = r#"
        MATCH (m:Movie)
        WHERE m.released > 2000 AND m.title CONTAINS 'Matrix'
        RETURN m.title
    "#;
    let rows = execute_query(&mut db, q).unwrap();
    // Should return only 'The Matrix Reloaded'
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0] { QueryResultRow::Info(s) => assert_eq!(s, "The Matrix Reloaded"), _ => panic!("expected Info with title") }
}

#[test]
fn cypher_where_multiple_equals_clauses() {
    let mut db = new_db();
    execute_query(&mut db, "CREATE (:Person {name: 'Tom Hanks', born: 1956});").unwrap();
    execute_query(&mut db, "CREATE (:Person {name: 'Tom Hardy', born: 1977});").unwrap();

    let q = r#"
        MATCH (p:Person)
        WHERE p.name = 'Tom Hanks' AND p.born = 1956
        RETURN p
    "#;
    let rows = execute_query(&mut db, q).unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0] {
        QueryResultRow::Node { label, metadata, .. } => {
            assert_eq!(label, "Person");
            assert_eq!(metadata.get("name").map(String::as_str), Some("Tom Hanks"));
            assert_eq!(metadata.get("born").map(String::as_str), Some("1956"));
        }
        _ => panic!("expected node row"),
    }
}

#[test]
fn cypher_variable_length_path_basic() {
    let mut db = new_db();
    // Create chain X1 -[:R]-> X2 -[:R]-> X3
    let rows = execute_query(&mut db, r#"
        CREATE (:X {name:'X1'});
        CREATE (:X {name:'X2'});
        CREATE (:X {name:'X3'});
    "#).unwrap();
    // Fetch ids for nodes to wire up relationships
    let all = execute_query(&mut db, "MATCH (n:X) RETURN n;").unwrap();
    let ids = ids_from_rows(&all.rows);
    assert_eq!(ids.len(), 3);

    // Sort by UUID to get stable order for wiring (not critical for test logic)
    let mut ids_sorted = ids.clone();
    ids_sorted.sort();
    let a = ids_sorted[0];
    let b = ids_sorted[1];
    let c = ids_sorted[2];

    // Create relationships A->B and B->C
    let q1 = format!("CREATE REL from={} to={} label=R;", a, b);
    let q2 = format!("CREATE REL from={} to={} label=R;", b, c);
    execute_query(&mut db, &q1).unwrap();
    execute_query(&mut db, &q2).unwrap();

    // Query variable-length 2 hops
    let out = execute_query(&mut db, r#"
        MATCH (s:X)-[:R*2]->(t:X)
        RETURN t
    "#).unwrap();
    // Expect exactly one result: node C
    assert_eq!(out.rows.len(), 1);
    match &out.rows[0] {
        QueryResultRow::Node { id, label, .. } => {
            assert_eq!(label, "X");
            assert_eq!(*id, c);
        }
        _ => panic!("expected node row"),
    }
}

#[test]
fn cypher_return_distinct_and_order_limit() {
    let mut db = new_db();
    // Create movies including duplicate titles
    execute_query(&mut db, "CREATE (:Movie {title: 'The Matrix'});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'The Matrix'});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'John Wick'});").unwrap();
    execute_query(&mut db, "CREATE (:Movie {title: 'Speed'});").unwrap();

    // DISTINCT titles should return unique list, ordered, limited to 2
    let q = r#"
        MATCH (m:Movie)
        RETURN DISTINCT m.title ORDER BY m.title ASC LIMIT 2
    "#;
    let rows = execute_query(&mut db, q).unwrap();
    assert_eq!(rows.rows.len(), 2);
    match (&rows.rows[0], &rows.rows[1]) {
        (QueryResultRow::Info(a), QueryResultRow::Info(b)) => {
            // Alphabetical order: 'John Wick', 'Speed', 'The Matrix'
            assert_eq!(a, "John Wick");
            assert_eq!(b, "Speed");
        }
        _ => panic!("expected Info rows with titles"),
    }
}

#[test]
fn cypher_set_remove_properties_and_labels() {
    let mut db = new_db();
    // Create a Person node
    execute_query(&mut db, "CREATE (:Person {name: 'Neo'});").unwrap();

    // Match it and set properties and label
    let q = r#"
        MATCH (p:Person {name:'Neo'})
        SET p.role = 'The One', p:Hero
        RETURN p
    "#;
    let rows = execute_query(&mut db, q).unwrap();
    assert_eq!(rows.rows.len(), 1);
    let nid = match &rows.rows[0] {
        QueryResultRow::Node { id, label, metadata } => {
            assert_eq!(label, "Hero");
            assert_eq!(metadata.get("name").map(String::as_str), Some("Neo"));
            assert_eq!(metadata.get("role").map(String::as_str), Some("The One"));
            *id
        }
        _ => panic!("expected node row"),
    };

    // Remove a property
    let q2 = r#"
        MATCH (p:Hero)
        WHERE id(p) = id(p)
        REMOVE p.role
        RETURN p
    "#;
    let rows2 = execute_query(&mut db, q2).unwrap();
    assert_eq!(rows2.rows.len(), 1);
    match &rows2.rows[0] {
        QueryResultRow::Node { id, label, metadata } => {
            assert_eq!(id.clone(), nid);
            assert_eq!(label, "Hero");
            assert!(metadata.get("role").is_none());
        }
        _ => panic!("expected node row"),
    }
}

#[test]
fn cypher_multiline_match_delete() {
    let mut db = new_db();
    // Seed nodes and relationships
    execute_query(
        &mut db,
        r#"
        CREATE NODE asdf {name:"n1"};
        CREATE NODE asdf {name:"n2"};
        "#,
    )
    .unwrap();
    // connect them
    let connect = "MATCH (a:asdf), (b:asdf) WHERE id(a) < id(b) MERGE (a)-[:RELATED_TO]->(b);";
    execute_query(&mut db, connect).unwrap();
    assert_eq!(db.relationship_count(), 1);

    // Now delete using multiline MATCH + DELETE
    let del = r#"
        MATCH (a:asdf)-[r:RELATED_TO]-(b:asdf)
        DELETE r;
    "#;
    execute_query(&mut db, del).unwrap();
    assert_eq!(db.relationship_count(), 0);
}

#[test]
fn cypher_multiline_match_where_merge() {
    let mut db = new_db();
    // Create 3 nodes
    execute_query(
        &mut db,
        r#"
        CREATE NODE asdf {name:"n1"};
        CREATE NODE asdf {name:"n2"};
        CREATE NODE asdf {name:"n3"};
        "#,
    )
    .unwrap();
    assert_eq!(db.node_count(), 3);

    // Multiline WHERE and MERGE
    let q = r#"
        MATCH (a:asdf), (b:asdf)
        WHERE id(a) < id(b)
        MERGE (a)-[:RELATED_TO]->(b);
    "#;
    let r = execute_query(&mut db, q).unwrap();
    assert!(r.mutated);
    assert_eq!(db.relationship_count(), 3);
}

#[test]
fn cypher_match_where_create_relationships() {
    let mut db = new_db();
    // Create 3 Text nodes
    execute_query(
        &mut db,
        r#"
        CREATE NODE Text {name:"t1"};
        CREATE NODE Text {name:"t2"};
        CREATE NODE Text {name:"t3"};
        "#,
    )
    .unwrap();
    assert_eq!(db.node_count(), 3);

    // Use MATCH..WHERE..CREATE to create pairwise IS_IN relationships
    let q = r#"
        MATCH (a:Text), (b:Text)
        WHERE id(a) < id(b)
        CREATE (a)-[:IS_IN]->(b);
    "#;
    let _ = execute_query(&mut db, q).expect("MATCH..WHERE..CREATE should work");
    // For 3 nodes with id(a) < id(b), expect 3 relationships
    assert_eq!(db.relationship_count(), 3);
}

#[test]
fn cypher_multiline_match_detach_delete_nodes() {
    let mut db = new_db();
    // Create 2 Person nodes and a relationship between them
    execute_query(
        &mut db,
        r#"
        CREATE NODE Person {name:"Ada"};
        CREATE NODE Person {name:"Bob"};
        "#,
    )
    .unwrap();
    // Connect them using Cypher MERGE
    let connect = r#"
        MATCH (a:Person), (b:Person)
        WHERE id(a) < id(b)
        MERGE (a)-[:RELATED_TO]->(b);
    "#;
    execute_query(&mut db, connect).unwrap();
    assert_eq!(db.node_count(), 2);
    assert_eq!(db.relationship_count(), 1);

    // Now delete all Person nodes via multiline MATCH + DETACH DELETE
    let del_nodes = r#"
        MATCH (n:Person)
        DETACH DELETE n
    "#;
    execute_query(&mut db, del_nodes).unwrap();
    assert_eq!(db.node_count(), 0);
    assert_eq!(db.relationship_count(), 0);
}

#[test]
fn cypher_multiline_create_comma_delimited() {
    let mut db = new_db();
    // CREATE with newline and comma-delimited patterns (including a trailing comma)
    let q = r#"
    CREATE
      (t1:T1 {name:'T1'}),
      (t2:T2 {name:'T2'}),
      (t3:T3 {name:'T3'}),
      (t4:T4 {name:'T4'}),
      (t5:T5 {name:'T5'}),
      (t6:T6 {name:'T6'}),
      (t7:T7 {name:'T7'}),
      (t8:T8 {name:'T8'}),
      (t9:T9 {name:'T9'}),
      (t10:T10 {name:'T10'}),
    "#;
    let out = execute_query(&mut db, q).expect("multiline CREATE should parse");
    // Should create 10 nodes
    assert!(out.mutated);
    assert_eq!(db.node_count(), 10);
    // Verify some labels exist
    let mut labels = Vec::new();
    for (_id, n) in db.nodes.iter() { labels.push(n.label.clone()); }
    labels.sort();
    assert!(labels.contains(&"T1".to_string()));
    assert!(labels.contains(&"T10".to_string()));
}
