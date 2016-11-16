use std::collections::HashMap;

use neo4j::bolt::{BoltStream};
use neo4j::packstream::Value;

#[macro_export]
macro_rules! parameters(
    {} => {
        {
            use std::collections::HashMap;

            HashMap::new()
        }
    };

    { $($key:expr => $value:expr),* } => {
        {
            use std::collections::HashMap;
            use $crate::neo4j::packstream::{Value, ValueCast};

            let mut map : HashMap<&str, Value> = HashMap::new();
            $(
                map.insert($key, ValueCast::from(&$value));
            )+;

            map
        }
    };
);
//
//struct DummyResponseHandler;
//impl BoltResponseHandler for DummyResponseHandler {
//    fn handle(&mut self, _: BoltResponse) {
//        //
//    }
//}
//
//struct InitResponseHandler {
//    summary: Option<BoltSummary>,
//}
//impl InitResponseHandler {
//    pub fn server(&self) -> Option<String> {
//        match self.summary {
//            Some(ref summary) => {
//                match summary {
//                    &BoltSummary::Success(ref fields) => {
//                        match fields[0] {
////                            Value::Map(map) => {
////                                match map.get("server") {
////                                    Value::String(s) => Some(s),
////                                    _ => panic!(),
////                                }
////                            },
//                            _ => panic!(),
//                        }
//                    },
//                    _ => panic!(),
//                }
//            },
//            None => None,
//        }
//    }
//}
//impl BoltResponseHandler for InitResponseHandler {
//    fn handle(&mut self, response: BoltResponse) {
//        match response {
//            BoltResponse::Summary(summary) => {
//                self.summary = Some(summary);
//            }
//            _ => panic!("Wrong type of thing!!")
//        }
//    }
//}


// GRAPH //

pub struct Graph {}
impl Graph {
    pub fn new(address: &str, user: &str, password: &str) -> Box<GraphConnector> {
        Box::new(DirectBoltConnector::new(address, user, password))
    }
}


// GRAPH CONNECTOR //

pub trait GraphConnector {
    fn connect(&self) -> Box<GraphConnection>;
}
struct DirectBoltConnector {
    address: String,
    user: String,
    password: String,
}
impl DirectBoltConnector {
    pub fn new(address: &str, user: &str, password: &str) -> DirectBoltConnector {
        DirectBoltConnector { address: String::from(address),
                       user: String::from(user), password: String::from(password) }
    }
}
impl GraphConnector for DirectBoltConnector {
    fn connect(&self) -> Box<GraphConnection> {
        Box::new(DirectBoltConnection::new(&self.address[..], &self.user[..], &self.password[..]))
    }
}


// GRAPH CONNECTION //

pub trait GraphConnection {
    fn server_version(&self) -> &str;
    fn begin(&mut self);
    fn commit(&mut self);
    fn reset(&mut self);
    fn rollback(&mut self);
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>);
    fn sync(&mut self);
}
struct DirectBoltConnection {
    connection: BoltStream,
    server_version: Option<String>,
}
impl DirectBoltConnection {
    pub fn new(address: &str, user: &str, password: &str) -> DirectBoltConnection {
        let mut connection = BoltStream::connect(address);
        let r = connection.pack_init(user, password);
        connection.sync();

        let server_version = match connection.metadata(r) {
            Some(ref metadata) => match metadata.get("server") {
                Some(ref server) => match *server {
                    &Value::String(ref string) => Some(string.clone()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };

        DirectBoltConnection {
            connection: connection,
            server_version: server_version,
        }
    }
}
impl GraphConnection for DirectBoltConnection {

    fn server_version(&self) -> &str {
        match self.server_version {
            Some(ref version) => &version[..],
            None => "",
        }
    }

    fn begin(&mut self) {
        self.connection.pack_run("BEGIN", parameters!());
        self.connection.pack_discard_all();
    }

    fn commit(&mut self) {
        self.connection.pack_run("COMMIT", parameters!());
        self.connection.pack_discard_all();
        self.connection.sync();
    }

    fn reset(&mut self) {
        self.connection.pack_reset();
        self.connection.sync();
    }

    fn rollback(&mut self) {
        self.connection.pack_run("ROLLBACK", parameters!());
        self.connection.pack_discard_all();
        self.connection.sync();
    }

    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        self.connection.pack_run(statement, parameters);
        self.connection.pack_pull_all();
    }

    fn sync(&mut self) {
        self.connection.sync();
    }
}

#[cfg(test)]
mod test {

}
