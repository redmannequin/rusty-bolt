#[macro_use]
extern crate log;

use std::collections::HashMap;

extern crate boltstream;
use boltstream::{BoltStream, BoltSummary, BoltError};

#[macro_use]
extern crate packstream;
use packstream::{Value, ValueCollection};

const USER_AGENT: &'static str = "rusty-bolt/0.1.0";

pub struct CypherStream {
    raw: BoltStream,
    server_version: Option<String>,
}
impl CypherStream {
    pub fn connect(address: &str, user: &str, password: &str) -> Result<CypherStream, BoltError> {
        info!("Connecting to bolt://{} as {}", address, user);
        match BoltStream::connect(address) {
            Ok(mut raw) => {
                raw.pack_init(USER_AGENT, user, password);
                let init = raw.collect_response();
                raw.send();
                let init_summary = raw.fetch_summary(init);
                let summary = init_summary.unwrap();
                raw.compact_responses();

                let server_version = match summary {
                    BoltSummary::Success(ref fields) => match fields.get(0) {
                        Some(&Value::Map(ref metadata)) => match metadata.get("server") {
                            Some(&Value::String(ref string)) => Some(string.clone()),
                            _ => None,
                        },
                        _ => None,
                    },
                    BoltSummary::Ignored(_) => panic!("Protocol violation! INIT should not be IGNORED"),
                    BoltSummary::Failure(_) => panic!("INIT returned FAILURE"),
                };

                info!("Connected to server version {:?}", server_version);
                Ok(CypherStream {
                    raw: raw,
                    server_version: server_version,
                })
            },
            Err(e) => Err(e)
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.raw.protocol_version()
    }

    pub fn server_version(&self) -> &str {
        match self.server_version {
            Some(ref version) => &version[..],
            None => "",
        }
    }

    pub fn begin_transaction(&mut self, bookmark: Option<String>) {
        info!("BEGIN {:?}->|...|", bookmark);
        self.raw.pack_run("BEGIN", match bookmark {
            Some(string) => parameters!("bookmark" => string),
            _ => parameters!(),
        });
        self.raw.pack_discard_all();
        self.raw.ignore_response();
        self.raw.ignore_response();
    }

    pub fn commit_transaction(&mut self) -> CommitResult {
        self.raw.pack_run("COMMIT", parameters!());
        self.raw.pack_discard_all();
        self.raw.ignore_response();
        let body = self.raw.collect_response();
        self.raw.send();
        let summary = self.raw.fetch_summary(body);
        self.raw.compact_responses();

        let bookmark: Option<String> = match summary {
            Some(BoltSummary::Success(ref fields)) => match fields.get(0) {
                Some(&Value::Map(ref metadata)) => match metadata.get("bookmark") {
                    Some(&Value::String(ref bookmark)) => Some(bookmark.clone()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };

        info!("COMMIT |...|->{:?}", bookmark);
        CommitResult { bookmark: bookmark }
    }

    pub fn rollback_transaction(&mut self) {
        self.raw.pack_run("ROLLBACK", parameters!());
        self.raw.pack_discard_all();
        self.raw.ignore_response();
        let body = self.raw.collect_response();
        self.raw.send();
        self.raw.fetch_summary(body);
        self.raw.compact_responses();
    }

    pub fn reset(&mut self) {
        self.raw.pack_reset();
        let reset = self.raw.collect_response();
        self.raw.send();
        self.raw.fetch_summary(reset);
        self.raw.compact_responses();
    }

    pub fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> Cursor {
        self.raw.pack_run(statement, parameters);
        self.raw.pack_pull_all();
        let head = self.raw.collect_response();
        let body = self.raw.collect_response();
        Cursor { head: head, body: body }
    }

    pub fn send(&mut self) {
        self.raw.send();
    }

    /// Fetch the result header summary
    pub fn fetch_header(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        let summary = self.raw.fetch_summary(cursor.head);
        info!("HEADER {:?}", summary);
        self.raw.compact_responses();
        match summary {
            Some(BoltSummary::Ignored(_)) => panic!("RUN was IGNORED"),
            Some(BoltSummary::Failure(_)) => {
                self.raw.pack_ack_failure();
                self.raw.ignore_response();
                self.raw.send();
            },
            _ => (),
        };
        summary
    }

    /// Fetch the result detail
    pub fn fetch_detail(&mut self, cursor: Cursor) -> Option<ValueCollection> {
        self.raw.fetch_detail(cursor.body)
    }

    /// Fetch the result footer summary
    pub fn fetch_footer(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        let summary = self.raw.fetch_summary(cursor.body);
        info!("FOOTER {:?}", summary);
        self.raw.compact_responses();
        match summary {
            Some(BoltSummary::Failure(_)) => {
                self.raw.pack_ack_failure();
                self.raw.ignore_response();
                self.raw.send();
            },
            _ => (),
        };
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
mod tests {
    #[test]
    fn it_works() {
    }
}
