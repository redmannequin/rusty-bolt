
use std::vec::Vec;
use std::collections::HashMap;

use neo4j::bolt::{BoltStream, Response};
use neo4j::packstream::Value;

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

pub struct GraphDatabase {}
impl GraphDatabase {
    pub fn driver(address: &str, user: &str, password: &str) -> Box<Driver> {
        Box::new(DirectDriver::new(address, user, password))
    }
}

pub trait Driver {
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

pub trait Session {
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

#[cfg(test)]
mod test {

}
