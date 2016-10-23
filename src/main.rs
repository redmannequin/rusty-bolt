#[macro_use]
extern crate log;
mod neo4j;

use std::collections::HashMap;
use log::{LogRecord, LogLevel, LogMetadata};
use neo4j::v1::GraphDatabase;

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
    {} => {
        HashMap::new()
    };

    { $($key:expr => $value:expr),* } => {
        {
            let mut map : HashMap<&str, neo4j::packstream::Value> = HashMap::new();
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

    let driver = GraphDatabase::driver("127.0.0.1:7687", "neo4j", "password");
    let mut session = driver.session();
    session.run("RETURN $x", parameters!("x" => vec!(-256, -255, -128, -127, -16, -15, -1, 0, 1, 15, 16, 127, 128, 255, 256)));
    session.run("RETURN $y", parameters!("y" => "hello, world"));
    session.run("UNWIND range(1, 3) AS n RETURN n", parameters!());
    session.sync();

}
