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
    connection: BoltStream,
    server_version: Option<String>,
}
impl DirectBoltConnection {
    pub fn new(address: &str, user: &str, password: &str) -> DirectBoltConnection {
        let mut connection = BoltStream::connect(address);

        let init = connection.pack_init(user, password);
        connection.send();
        connection.fetch_summary(init);
//        let server_version = match connection.response_metadata(init) {
//            Some(ref metadata) => match metadata.get("server") {
//                Some(ref server) => match *server {
//                    &Value::String(ref string) => Some(string.clone()),
//                    _ => None,
//                },
//                _ => None,
//            },
//            _ => None,
//        };
//        connection.response_done(init);

        DirectBoltConnection {
            connection: connection,
            server_version: None,
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
        self.connection.pack_run("BEGIN", parameters!());
        let body = self.connection.pack_discard_all();
        // TODO: mark as "done" without fetching summary (discard response or something)
        self.connection.fetch_summary(body);
    }

    fn commit(&mut self) -> CommitResult {
        self.connection.pack_run("COMMIT", parameters!());
        let body = self.connection.pack_discard_all();
        self.connection.send();
        let summary = self.connection.fetch_summary(body);
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
//        self.connection.response_done(header);
//        self.connection.response_done(footer);
        CommitResult { bookmark: bookmark }
    }

    fn reset(&mut self) {
        let reset = self.connection.pack_reset();
        self.connection.send();
        self.connection.fetch_summary(reset);
    }

    fn rollback(&mut self) {
        self.connection.pack_run("ROLLBACK", parameters!());
        let body = self.connection.pack_discard_all();
        self.connection.send();
        self.connection.fetch_summary(body);
    }

    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> Cursor {
        let head = self.connection.pack_run(statement, parameters);
        let body = self.connection.pack_pull_all();
        Cursor { head: head, body: body }
    }

    fn send(&mut self) {
        self.connection.send();
    }

    fn head(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        self.connection.fetch_summary(cursor.head)
    }

    fn next(&mut self, cursor: Cursor) -> Option<BoltDetail> {
        self.connection.fetch_detail(cursor.body)
    }

    fn done(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        self.connection.fetch_summary(cursor.body)
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
