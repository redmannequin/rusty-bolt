use std::collections::HashMap;

use neo4j::bolt::BoltStream;
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
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>);
    fn sync(&mut self);
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
        connection.sync();
        let server_version = match connection.metadata(init) {
            Some(ref metadata) => match metadata.get("server") {
                Some(ref server) => match *server {
                    &Value::String(ref string) => Some(string.clone()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };
        connection.done(init);

        DirectBoltConnection {
            connection: connection,
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
        let header = self.connection.pack_run("BEGIN", parameters!());
        let footer = self.connection.pack_discard_all();
        self.connection.done(header);
        self.connection.done(footer);
    }

    fn commit(&mut self) -> CommitResult {
        let header = self.connection.pack_run("COMMIT", parameters!());
        let footer = self.connection.pack_discard_all();
        self.connection.sync();
        let bookmark: Option<String> = match self.connection.metadata(footer) {
            Some(metadata) => match metadata.get("bookmark") {
                Some(value) => match value {
                    &Value::String(ref bookmark) => Some(bookmark.clone()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };
        self.connection.done(header);
        self.connection.done(footer);
        CommitResult { bookmark: bookmark }
    }

    fn reset(&mut self) {
        let reset = self.connection.pack_reset();
        self.connection.sync();
        self.connection.done(reset);
    }

    fn rollback(&mut self) {
        let header = self.connection.pack_run("ROLLBACK", parameters!());
        let footer = self.connection.pack_discard_all();
        self.connection.sync();
        self.connection.done(header);
        self.connection.done(footer);
    }

    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        self.connection.pack_run(statement, parameters);
        self.connection.pack_pull_all();
    }

    fn sync(&mut self) {
        self.connection.sync();
    }
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
