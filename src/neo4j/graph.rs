use std::collections::HashMap;

use neo4j::bolt::{BoltStream, BoltDetail, BoltSummary};
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


// GRAPH //

pub trait Graph {
    fn server_version(&self) -> &str;
    fn begin(&mut self);
    fn commit(&mut self) -> CommitResult;
    fn reset(&mut self);
    fn rollback(&mut self);
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> Cursor;
    fn send(&mut self);
    fn head(&mut self, cursor: Cursor) -> Option<BoltSummary>;
    fn next(&mut self, cursor: Cursor) -> Option<BoltDetail>;
    fn done(&mut self, cursor: Cursor) -> Option<BoltSummary>;
}
impl Graph {
    pub fn connect(address: &str, user: &str, password: &str) -> Box<Graph> {
        Box::new(DirectBoltConnection::new(&address[..], &user[..], &password[..]))
    }
}

struct DirectBoltConnection {
    bolt: BoltStream,
    server_version: Option<String>,
}
impl DirectBoltConnection {
    pub fn new(address: &str, user: &str, password: &str) -> DirectBoltConnection {
        let mut bolt = BoltStream::connect(address);

        bolt.pack_init(user, password);
        let init = bolt.collect_response();
        bolt.send();
        let summary = bolt.fetch_summary(init).unwrap();
        bolt.compact_responses();

        let server_version = match summary {
            BoltSummary::Success(ref fields) => match fields.get(0) {
                Some(value) => match value {
                    &Value::Map(ref metadata) => match metadata.get("server") {
                        Some(server) => match *server {
                            Value::String(ref string) => Some(string.clone()),
                            _ => None,
                        },
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            },
            BoltSummary::Ignored(_) => None,
            BoltSummary::Failure(_) => None,
        };

        DirectBoltConnection {
            bolt: bolt,
            server_version: server_version,
        }
    }
}
impl Graph for DirectBoltConnection {

    fn server_version(&self) -> &str {
        match self.server_version {
            Some(ref version) => &version[..],
            None => "",
        }
    }

    fn begin(&mut self) {
        self.bolt.pack_run("BEGIN", parameters!());
        self.bolt.pack_discard_all();
        self.bolt.ignore_response();
        self.bolt.ignore_response();
    }

    fn commit(&mut self) -> CommitResult {
        self.bolt.pack_run("COMMIT", parameters!());
        self.bolt.pack_discard_all();
        self.bolt.ignore_response();
        let body = self.bolt.collect_response();
        self.bolt.send();
        let summary = self.bolt.fetch_summary(body);
        self.bolt.compact_responses();
        let bookmark: Option<String> = match summary {
            Some(summary) => match summary {
                BoltSummary::Success(ref fields) => match fields.get(0) {
                    Some(ref field) => match *field {
                        &Value::Map(ref metadata) => match metadata.get("bookmark") {
                            Some(value) => match value {
                                &Value::String(ref bookmark) => Some(bookmark.clone()),
                                _ => None,
                            },
                            _ => None,
                        },
                        _ => None,
                    },
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };
        CommitResult { bookmark: bookmark }
    }

    fn reset(&mut self) {
        self.bolt.pack_reset();
        let reset = self.bolt.collect_response();
        self.bolt.send();
        self.bolt.fetch_summary(reset);
        self.bolt.compact_responses();
    }

    fn rollback(&mut self) {
        self.bolt.pack_run("ROLLBACK", parameters!());
        self.bolt.pack_discard_all();
        self.bolt.ignore_response();
        let body = self.bolt.collect_response();
        self.bolt.send();
        self.bolt.fetch_summary(body);
        self.bolt.compact_responses();
    }

    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> Cursor {
        self.bolt.pack_run(statement, parameters);
        self.bolt.pack_pull_all();
        let head = self.bolt.collect_response();
        let body = self.bolt.collect_response();
        Cursor { head: head, body: body }
    }

    fn send(&mut self) {
        self.bolt.send();
    }

    fn head(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        let summary = self.bolt.fetch_summary(cursor.head);
        self.bolt.compact_responses();
        summary
    }

    fn next(&mut self, cursor: Cursor) -> Option<BoltDetail> {
        self.bolt.fetch_detail(cursor.body)
    }

    fn done(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        let summary = self.bolt.fetch_summary(cursor.body);
        self.bolt.compact_responses();
        summary
    }
}

#[derive(Copy, Clone)]
pub struct Cursor {
    head: usize,
    body: usize,
}

pub struct CommitResult {
    bookmark: Option<String>,
}
impl CommitResult {
    pub fn bookmark(&self) -> Option<&str> {
        match self.bookmark {
            Some(ref bookmark) => Some(&bookmark[..]),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {

}
