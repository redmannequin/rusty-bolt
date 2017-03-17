//use std::io::{stderr, Write};
use std::env;
use std::collections::VecDeque;

//#[macro_use]
//extern crate log;
//use log::{LogRecord, LogLevel, LogMetadata};

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

#[macro_use]
extern crate cypherstream;
use cypherstream::{CypherStream};

#[macro_use]
extern crate boltstream;
use boltstream::{BoltSummary};

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
    cypher.send();

    match cypher.fetch_header(result) {
        Some(header) => match header {
            BoltSummary::Success(ref metadata) => println!("{}", metadata.get("fields").unwrap()),
            _ => panic!("Failed! Not successful."),
        },
        _ => panic!("Failed! No header summary"),
    }

    // iterate result
    let mut data_stream: VecDeque<Data> = VecDeque::new();
    while cypher.fetch_data(result, &mut data_stream) > 0 {
        for data in data_stream.drain(..) {
            println!("{}", data);
        }
    }

    // close result
    let _ = cypher.fetch_footer(result);

    // commit transaction
    let commit_result = cypher.commit_transaction();
    let _ = commit_result.bookmark();

}
