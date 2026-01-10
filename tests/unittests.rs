

use graph_loom::gql::query_interface::{execute_query, QueryOutcome, QueryResultRow};
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
