use std::vec::Vec;
use std::collections::HashMap;

use neo4j::bolt::{BoltStream, Response};
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

struct DummyResponse;
impl Response for DummyResponse {
    fn on_success(&self, _: &HashMap<String, Value>) {
        //
    }

    fn on_record(&self, _: &Vec<Value>) {
        //
    }

    fn on_ignored(&self, _: &HashMap<String, Value>) {
        //
    }

    fn on_failure(&self, _: &HashMap<String, Value>) {
        //
    }
}


// GRAPH DATABASE //

pub struct GraphDatabase {}
impl GraphDatabase {
    pub fn driver(address: &str, user: &str, password: &str) -> Box<Driver> {
        Box::new(DirectDriver::new(address, user, password))
    }
}


// DRIVER //

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


// SESSION //

pub trait Session {
    //fn begin(&mut self) -> Box<Transaction<'t>>;
    fn reset(&mut self);
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>);
    fn sync(&mut self);
}
struct NetworkSession {
    connection: BoltStream,
    //transaction: Option<Box<Transaction<'t>>>,
}
impl NetworkSession {
    pub fn new(address: &str, user: &str, password: &str) -> NetworkSession {
        let mut connection = BoltStream::connect(address);
        connection.pack_init(user, password, DummyResponse {});
        connection.sync();

        NetworkSession { connection: connection, /*transaction: None*/ }
    }
}
impl Session for NetworkSession {
//    fn begin(&mut self) -> Box<Transaction<'t>> {
//        let tx = Box::new(ExplicitTransaction::new(self));
//        self.transaction = Some(tx);
//        tx
//    }

    fn reset(&mut self) {
        self.connection.pack_reset(DummyResponse {});
        self.connection.sync();
    }

    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        self.connection.pack_run(statement, parameters, DummyResponse {});
        self.connection.pack_pull_all(DummyResponse {});
    }

    fn sync(&mut self) {
        self.connection.sync();
    }
}


// TRANSACTION //

//pub trait Transaction<'t> {
//    fn taint(&mut self);
//}
//struct ExplicitTransaction<'t> {
//    session: &'t mut Session<'t>,
//    tainted: bool,
//}
//impl<'t> ExplicitTransaction<'t> {
//    fn new(session: &mut Session) -> ExplicitTransaction<'t> {
//        session.run("BEGIN", parameters!());
//        ExplicitTransaction {session: session, tainted: false}
//    }
//}
//impl<'t> Transaction<'t> for ExplicitTransaction<'t> {
//    fn taint(&mut self) {
//        self.tainted = true;
//    }
//}
//impl<'t> Drop for ExplicitTransaction<'t> {
//    fn drop(&mut self) {
//        if self.tainted {
//            self.session.run("ROLLBACK", parameters!());
//        }
//        else {
//            self.session.run("COMMIT", parameters!());
//        }
//        self.session.sync();
//    }
//}

#[cfg(test)]
mod test {

}
