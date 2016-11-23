use std::io::{stderr, Write};
//use std::env;

#[macro_use]
extern crate log;
use log::{LogRecord, LogLevel, LogMetadata};

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= LogLevel::Info
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            let _ = writeln!(stderr(), "[{}]  {}", record.level(), record.args());
        }
    }
}

//////////////////////////////////////////////////////////////////////

#[macro_use]
mod neo4j;
use neo4j::graph::{Graph};
use neo4j::bolt::BoltDetail;  // TODO encapsulate

fn main() {
    let statement = String::from("UNWIND range(1, 3) AS n RETURN n");
    let parameters = parameters!();

    let _ = log::set_logger(|max_log_level| {
        max_log_level.set(log::LogLevelFilter::Info);
        Box::new(SimpleLogger)
    });


    // connect
    let mut graph = Graph::connect("127.0.0.1:7687", "neo4j", "password");
    println!("Connected to server {}", graph.server_version());

    // begin transaction
    graph.begin();

    // execute statement
    let cursor = graph.run(&statement[..], parameters);
    graph.send();

    let header = graph.fetch_header(cursor);
    println!("HEAD {:?}", header);

    // iterate result
    let mut record: Option<BoltDetail> = graph.fetch(cursor);
    while record.is_some() {
        println!("BODY {:?}", record);
        record = graph.fetch(cursor);
    }

    // close result
    let summary = graph.fetch_summary(cursor);
    println!("FOOT {:?}", summary);

    // commit transaction
    let commit_result = graph.commit();
    println!("Bookmark {:?}", commit_result.bookmark());

}
