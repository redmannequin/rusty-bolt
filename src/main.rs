#[macro_use]
extern crate log;

use std::vec::Vec;
use std::collections::HashMap;

mod neo4j;
use neo4j::bolt::{BoltStream, Response};
use neo4j::packstream::{Value, ValueCast};

struct LoggingResponse;
impl Response for LoggingResponse {
    fn on_success(&self, metadata: &HashMap<String, Value>) {
        info!("S: SUCCESS {:?}", metadata);
    }

    fn on_record(&self, data: &Vec<Value>) {
        info!("S: RECORD {:?}", data);
    }

    fn on_ignored(&self, metadata: &HashMap<String, Value>) {
        info!("S: IGNORED {:?}", metadata);
    }

    fn on_failure(&self, metadata: &HashMap<String, Value>) {
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
    {} => {
        HashMap::new()
    };

    { $($key:expr => $value:expr),* } => {
        {
            let mut map : HashMap<&str, Value> = HashMap::new();
            $(
                map.insert($key, ValueCast::from(&$value));
            )+;
            map
        }
    };
);

struct GraphDatabase {}
impl GraphDatabase {
    pub fn driver(address: &str, user: &str, password: &str) -> Box<Driver> {
        Box::new(DirectDriver::new(address, user, password))
    }
}

trait Driver {
    fn session(&self) -> Box<Session>;
}
struct DirectDriver {
    address: String,
    user: String,
    password: String,
}
impl DirectDriver {
    pub fn new(address: &str, user: &str, password: &str) -> DirectDriver {
        DirectDriver { address: String::from(address),
                       user: String::from(user), password: String::from(password) }
    }
}
impl Driver for DirectDriver {
    fn session(&self) -> Box<Session> {
        Box::new(NetworkSession::new(&self.address[..], &self.user[..], &self.password[..]))
    }
}

trait Session {
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>);
    fn sync(&mut self);
}
struct NetworkSession {
    connection: BoltStream,
}
impl NetworkSession {
    pub fn new(address: &str, user: &str, password: &str) -> NetworkSession {
        let mut connection = BoltStream::connect(address);
        connection.pack_init(user, password, LoggingResponse {});
        connection.sync();
        NetworkSession { connection: connection }
    }

}
impl Session for NetworkSession {
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        self.connection.pack_run(statement, parameters, LoggingResponse {});
        self.connection.pack_pull_all(LoggingResponse {});
    }

    fn sync(&mut self) {
        self.connection.sync();
    }
}

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
