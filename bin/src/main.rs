use std::collections::HashMap;
use std::env;

use neo4j::*;
use packstream::{parameters, Value};

fn main() {
    let mut args = env::args();

    let statement = match args.nth(1) {
        Some(string) => string,
        _ => String::from("RETURN $x"),
    };
    let parameters = parameters!("x" => 1);

    let session = Neo4jDB::connect("[::1]:7687", "neo4j", "password").unwrap();
    dump(session, &statement[..], parameters);
}

fn dump(mut neo: Neo4jDB, statement: &str, parameters: HashMap<&str, Value>) {
    // execute statement
    let result = neo.run(statement, parameters).unwrap();

    // iterate result
    let mut counter: usize = 0;
    for record in result {
        println!("{:?}", record);
        counter += 1;
    }

    println!(
        "({} record{})",
        counter,
        match counter {
            1 => "",
            _ => "s",
        }
    );
}
