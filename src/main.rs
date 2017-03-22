//////////////// LOGGING ///////////////////

//use std::io::{stderr, Write};
//
//#[macro_use]
//extern crate log;
//use log::{LogRecord, LogLevel, LogMetadata};
//
//struct SimpleLogger;
//
//impl log::Log for SimpleLogger {
//    fn enabled(&self, metadata: &LogMetadata) -> bool {
//        metadata.level() <= LogLevel::Debug
//    }
//
//    fn log(&self, record: &LogRecord) {
//        if self.enabled(record.metadata()) {
//            let _ = writeln!(stderr(), "[{}]  {}", record.level(), record.args());
//        }
//    }
//}

//////////////////////////////////////////////////////////////////////

use std::env;
use std::collections::VecDeque;

#[macro_use]
extern crate cypherstream;
use cypherstream::{CypherStream};

#[macro_use]
extern crate packstream;
use packstream::{Data};

fn main() {
    let mut args = env::args();

    let statement = match args.nth(1) {
        Some(string) => string,
        _ => String::from("UNWIND range(1, 10) AS n RETURN n, n * n AS n_sq, 'no ' + toString(n) AS n_str"),
    };
    let parameters = parameters!();

//    let _ = log::set_logger(|max_log_level| {
//        max_log_level.set(log::LogLevelFilter::Debug);
//        Box::new(SimpleLogger)
//    });

    // connect
    let address = "[::1]:7687";
    let user = "neo4j";
    let password = "password";
    let mut cypher = CypherStream::connect(address, user, password).unwrap();

    // begin transaction
    cypher.begin_transaction(None);

    // execute statement
    let result = cypher.run(&statement[..], parameters);
    println!("{}", result.keys());

    // iterate result
    let mut records: VecDeque<Data> = VecDeque::new();
    while cypher.fetch(&result, &mut records) > 0 {
        for record in records.drain(..) {
            println!("{}", record);
        }
    }

    // close result
    let _ = cypher.fetch_summary(result);

    // commit transaction
    cypher.commit_transaction();
    println!("({} records at bookmark {:?})", 0, cypher.last_bookmark());

}
