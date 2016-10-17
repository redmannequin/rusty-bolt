#[macro_use]
extern crate log;

use std::vec::Vec;
use std::collections::HashMap;

mod neo4j;
use neo4j::bolt;
use neo4j::packstream;

struct LoggingResponse;
impl bolt::Response for LoggingResponse {
    fn on_success(&self, metadata: &HashMap<String, packstream::Value>) {
        info!("S: SUCCESS {:?}", metadata);
    }

    fn on_record(&self, data: &Vec<packstream::Value>) {
        info!("S: RECORD {:?}", data);
    }

    fn on_ignored(&self, metadata: &HashMap<String, packstream::Value>) {
        info!("S: IGNORED {:?}", metadata);
    }

    fn on_failure(&self, metadata: &HashMap<String, packstream::Value>) {
        info!("S: FAILURE {:?}", metadata);
    }
}

use log::{LogRecord, LogLevel, LogMetadata};

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= LogLevel::Info
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            println!("[{}]  {}", record.level(), record.args());
        }
    }
}

macro_rules! parameters(
    { $($key:expr => $value:expr),* } => {
        {
            let mut map : std::collections::HashMap<&str, neo4j::packstream::Value> = std::collections::HashMap::new();
            $(
                map.insert($key, neo4j::packstream::ValueCast::from(&$value));
            )+;
            map
        }
     };
);

fn main() {
    let _ = log::set_logger(|max_log_level| {
        max_log_level.set(log::LogLevelFilter::Info);
        Box::new(SimpleLogger)
    });

    let mut bolt = bolt::BoltStream::connect("127.0.0.1:7687");

    bolt.pack_init("neo4j", "password", LoggingResponse {});
    bolt.sync();

    bolt.pack_run("RETURN 1, $x, $y", parameters!("x" => 42, "y" => "hello"), LoggingResponse {});
    bolt.pack_pull_all(LoggingResponse {});
    bolt.sync();

}
