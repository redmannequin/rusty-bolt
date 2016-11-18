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
    fn commit(&mut self);
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
        connection.done(r);

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
