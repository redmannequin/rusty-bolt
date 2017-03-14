use std::io::{stderr, Write};
use std::env;

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
extern crate boltstream;
use boltstream::{Bolt, BoltDetail, BoltSummary};

extern crate packstream;
use packstream::Value;

fn main() {
    let mut args = env::args();

    let statement = match args.nth(1) {
        Some(string) => string,
        _ => String::from("UNWIND range(1, 10) AS n RETURN n, n * n AS n_sq, 'no ' + toString(n) AS n_str"),
    };
    let parameters = parameters!();

    let _ = log::set_logger(|max_log_level| {
        max_log_level.set(log::LogLevelFilter::Info);
        Box::new(SimpleLogger)
    });

    // connect
    let address = "[::1]:7687";
    let user = "neo4j";
    let password = "password";
    let _ = writeln!(stderr(), "Connecting to bolt://{} as {}", address, user);
    let mut bolt = Bolt::connect(address, user, password);
    let _ = writeln!(stderr(), "Connected to {}", bolt.server_version());

    // begin transaction
    bolt.begin(None);

    // execute statement
    let cursor = bolt.run(&statement[..], parameters);
    bolt.send();

    match bolt.fetch_header(cursor) {
        Some(header) => match header {
            BoltSummary::Success(ref values) => match values[0] {
                Value::Map(ref map) => println!("{}", map.get("fields").unwrap()),
                _ => panic!("Failed! Not a map."),
            },
            _ => panic!("Failed! Not successful."),
        },
        _ => panic!("Failed! No header summary"),
    }

    // iterate result
    let mut sleeve: Option<BoltDetail> = bolt.fetch_detail(cursor);
    while sleeve.is_some() {
        match sleeve {
            Some(ref record) => println!("{}", record),
            _ => (),
        }
        sleeve = bolt.fetch_detail(cursor);
    }

    // close result
    let _ = bolt.fetch_footer(cursor);

    // commit transaction
    let commit_result = bolt.commit();
    let _ = commit_result.bookmark();

}
