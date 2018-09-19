//////////////// LOGGING ///////////////////

use std::io::{stderr, Write};

extern crate log;
use log::{LogRecord, LogLevel, LogMetadata};

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= LogLevel::Debug
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            let _ = writeln!(stderr(), "[{}]  {}", record.level(), record.args());
        }
    }
}

//////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::env;

use neo4j::*;
use packstream::{parameters, Value};

fn main() {
    let mut args = env::args();

    let statement = match args.nth(1) {
        Some(string) => string,
        _ => String::from("MERGE p=(a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN a, r, p"),
    };
    let parameters = parameters!("x" => 1);

    //    let _ = log::set_logger(|max_log_level| {
    //        max_log_level.set(log::LogLevelFilter::Debug);
    //        Box::new(SimpleLogger)
    //    });

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
