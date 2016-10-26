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
            println!("[{}]  {}", record.level(), record.args());
        }
    }
}

//////////////////////////////////////////////////////////////////////

#[macro_use]
mod neo4j;
use neo4j::graph::Graph;

fn main() {
    let _ = log::set_logger(|max_log_level| {
        max_log_level.set(log::LogLevelFilter::Info);
        Box::new(SimpleLogger)
    });

    let graph = Graph::new("127.0.0.1:7687", "neo4j", "password");
    let mut connection = graph.connect();
    connection.begin();
    connection.run("CREATE (a:Person {name:'Alice'}) RETURN a", parameters!());
    connection.commit();

//    connection.run("RETURN $x", parameters!("x" => vec!(-256, -255, -128, -127, -16, -15, -1, 0, 1, 15, 16, 127, 128, 255, 256)));
//    connection.run("RETURN $y", parameters!("y" => "hello, world"));
//    connection.run("UNWIND range(1, 3) AS n RETURN n", parameters!());

    connection.sync();
}
