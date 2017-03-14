use std::fmt;
use std::vec::Vec;
use std::collections::{HashMap, VecDeque};
use std::io::prelude::*;
use std::io::{stderr, Write};
use std::net::{TcpStream, ToSocketAddrs};

extern crate packstream;
use packstream::{Value, Packer, Unpacker, write_tsv};

const HANDSHAKE: [u8; 20] = [0x60, 0x60, 0xB0, 0x17,
                             0x00, 0x00, 0x00, 0x01,
                             0x00, 0x00, 0x00, 0x00,
                             0x00, 0x00, 0x00, 0x00,
                             0x00, 0x00, 0x00, 0x00];

const MAX_CHUNK_SIZE: usize = 0xFFFF;
const USER_AGENT: &'static str = "rusty-bolt/0.1.0";

pub struct BoltStream {
    stream: TcpStream,
    packer: Packer,
    unpacker: Unpacker,
    request_markers: VecDeque<usize>,
    responses: VecDeque<BoltResponse>,
    responses_done: usize,
    current_response_index: usize,
    protocol_version: u32,
}

impl BoltStream {
    pub fn connect<A: ToSocketAddrs>(address: A) -> BoltStream {
        match TcpStream::connect(address) {
            Ok(mut stream) => match stream.write(&HANDSHAKE) {
                Ok(_) => {
                    let mut buf = [0; 4];
                    match stream.read(&mut buf) {
                        Ok(_) => {
                            let version: u32 = (buf[0] as u32) << 24 |
                                               (buf[1] as u32) << 16 |
                                               (buf[2] as u32) << 8 |
                                               (buf[3] as u32);
                            //info!("S: <VERSION {}>", version)
                            BoltStream { stream: stream, packer: Packer::new(), unpacker: Unpacker::new(),
                                         request_markers: VecDeque::new(), responses: VecDeque::new(),
                                         responses_done: 0, current_response_index: 0, protocol_version: version }
                        },
                        Err(e) => panic!("Got an error on read: {}", e),
                    }
                },
                Err(e) => panic!("Got an error on write: {}", e),
            },
            Err(e) => panic!("Got an error on connect: {}", e),
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Pack an INIT message.
    ///
    pub fn pack_init(&mut self, user: &str, password: &str) {
        //info!("C: INIT {:?} {{\"scheme\": \"basic\", \"principal\": {:?}, \"credentials\": \"...\"}}", USER_AGENT, user);
        self.packer.pack_structure_header(2, 0x01);
        self.packer.pack_string(USER_AGENT);
        self.packer.pack_map_header(3);
        self.packer.pack_string("scheme");
        self.packer.pack_string("basic");
        self.packer.pack_string("principal");
        self.packer.pack_string(user);
        self.packer.pack_string("credentials");
        self.packer.pack_string(password);
        self.request_markers.push_back(self.packer.len());
    }

    /// Pack an ACK_FAILURE message.
    ///
    pub fn pack_ack_failure(&mut self) {
        //info!("C: ACK_FAILURE");
        self.packer.pack_structure_header(0, 0x0E);
        self.request_markers.push_back(self.packer.len());
    }

    /// Pack a RESET message.
    ///
    pub fn pack_reset(&mut self) {
        //info!("C: RESET");
        self.packer.pack_structure_header(0, 0x0F);
        self.request_markers.push_back(self.packer.len());
    }

    /// Pack a RUN message.
    ///
    pub fn pack_run(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        //info!("C: RUN {:?} {:?}", statement, parameters);
        self.packer.pack_structure_header(2, 0x10);
        self.packer.pack_string(statement);
        self.packer.pack_map_header(parameters.len());
        for (name, value) in &parameters {
            self.packer.pack_string(name);
            self.packer.pack(value);
        }
        self.request_markers.push_back(self.packer.len());
    }

    /// Pack a DISCARD_ALL message.
    ///
    pub fn pack_discard_all(&mut self) {
        //info!("C: DISCARD_ALL");
        self.packer.pack_structure_header(0, 0x2F);
        self.request_markers.push_back(self.packer.len());
    }

    /// Pack a PULL_ALL message.
    ///
    pub fn pack_pull_all(&mut self) {
        //info!("C: PULL_ALL");
        self.packer.pack_structure_header(0, 0x3F);
        self.request_markers.push_back(self.packer.len());
    }

    /// Send all queued outgoing messages.
    ///
    pub fn send(&mut self) {
        //info!("C: <SEND>");
        let mut offset: usize = 0;
        for &mark in &self.request_markers {
            for chunk_data in self.packer[offset..mark].chunks(MAX_CHUNK_SIZE) {
                let chunk_size = chunk_data.len();
                let _ = self.stream.write(&[(chunk_size >> 8) as u8, chunk_size as u8]).unwrap();
                let _ = self.stream.write(&chunk_data).unwrap();
            }
            let _ = self.stream.write(&[0, 0]).unwrap();
            offset = mark;
        }
        self.packer.clear();
        self.request_markers.clear();
    }

    pub fn collect_response(&mut self) -> usize {
        self.responses.push_back(BoltResponse::new());
        self.responses_done + self.responses.len() - 1
    }

    pub fn ignore_response(&mut self) {
        self.responses.push_back(BoltResponse::done());
    }

    // TODO: hook in pruning
    pub fn compact_responses(&mut self) {
        let mut pruning = true;
        while pruning && self.current_response_index > 0 {
            match self.responses.front() {
                Some(response) => {
                    if !response.done {
                        pruning = false;
                    }
                },
                _ => {
                    pruning = false;
                },
            }
            if pruning {
                self.responses.pop_front();
                self.responses_done += 1;
                self.current_response_index -= 1;
            }
        }
    }

    /// Fetches the next response message for the designated response,
    /// assuming that response is not already completely buffered.
    ///
    pub fn fetch_detail(&mut self, response_id: usize) -> Option<BoltDetail> {
        let response_index = response_id - self.responses_done;
        while self.current_response_index < response_index {
            self.read_message();
        }
        if self.current_response_index == response_index {
            self.read_message();
        }
        self.responses[response_index].detail.pop_front()
    }

    /// Fetches all response messages for the designated response,
    /// assuming that response is not already completely buffered.
    ///
    pub fn fetch_summary(&mut self, response_id: usize) -> Option<BoltSummary> {
        let response_index = response_id - self.responses_done;
        while self.current_response_index <= response_index {
            self.read_message();
        }
        let ref mut response = self.responses[response_index];
        response.done = true;
        response.summary.take()
    }

    /// Reads the next message from the stream into the read buffer.
    ///
    fn read_message(&mut self) -> u8 {
        self.unpacker.clear();
        let mut chunk_size: usize = self.read_chunk_size();
        while chunk_size > 0 {
            let _ = self.stream.read_exact(&mut self.unpacker.buffer(chunk_size));
            chunk_size = self.read_chunk_size();
        }

        let message: Value = self.unpacker.unpack();
        match message {
            Value::Structure { signature, fields } => {
                match signature {
                    0x70 => {
                        //info!("S: SUCCESS {:?}", fields[0]);
                        let mut response = self.responses.get_mut(self.current_response_index).unwrap();
                        response.summary = Some(BoltSummary::Success(fields));
                        self.current_response_index += 1;
                    },
                    0x71 => {
                        //info!("S: RECORD {:?}", fields[0]);
                        let mut response = self.responses.get_mut(self.current_response_index).unwrap();
                        response.detail.push_back(BoltDetail::Record(fields));
                    },
                    0x7E => {
                        //info!("S: IGNORED {:?}", fields[0]);
                        let mut response = self.responses.get_mut(self.current_response_index).unwrap();
                        response.summary = Some(BoltSummary::Ignored(fields));
                        self.current_response_index += 1;
                    },
                    0x7F => {
                        //info!("S: FAILURE {:?}", fields[0]);
                        {
                            let mut response = self.responses.get_mut(self.current_response_index).unwrap();
                            response.summary = Some(BoltSummary::Failure(fields));
                        }
                        self.current_response_index += 1;
                    },
                    _ => panic!("Unknown response message with signature {:02X}", signature),
                }
                return signature;
            },
            _ => panic!("Response message is not a structure"),
        }
    }

    fn read_chunk_size(&mut self) -> usize {
        let mut chunk_header = &mut [0u8; 2];
        let _ = self.stream.read_exact(chunk_header);
//        log_line!("S: [{:02X} {:02X}]", chunk_header[0] as u8, chunk_header[1] as u8);
        0x100 * chunk_header[0] as usize + chunk_header[1] as usize
    }

}

pub enum BoltDetail {
    Record(Vec<Value>),
}
impl fmt::Debug for BoltDetail {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BoltDetail::Record(ref values) => write!(f, "Record({:?})", values),
        }
    }
}
impl fmt::Display for BoltDetail {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BoltDetail::Record(ref fields) => match fields.len() {
                0 => write!(f, ""),
                _ => match fields[0] {
                    Value::List(ref values) => write_tsv(f, values),
                    _ => write!(f, ""),
                },
            },
        }
    }
}

pub enum BoltSummary {
    Success(Vec<Value>),
    Ignored(Vec<Value>),
    Failure(Vec<Value>),
}
impl fmt::Debug for BoltSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BoltSummary::Success(ref values) => write!(f, "Success({:?})", values),
            BoltSummary::Ignored(ref values) => write!(f, "Ignored({:?})", values),
            BoltSummary::Failure(ref values) => write!(f, "Failure({:?})", values),
        }
    }
}

pub struct BoltResponse {
    detail: VecDeque<BoltDetail>,
    summary: Option<BoltSummary>,
    done: bool,
}
impl BoltResponse {
    pub fn new() -> BoltResponse {
        BoltResponse { detail: VecDeque::new(), summary: None, done: false }
    }
    pub fn done() -> BoltResponse {
        BoltResponse { detail: VecDeque::new(), summary: None, done: true }
    }
}

impl fmt::Debug for BoltResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.summary {
            Some(ref summary) => {
                match *summary {
                    BoltSummary::Success(ref metadata) => write!(f, "SUCCESS {:?}", metadata),
                    BoltSummary::Ignored(ref metadata) => write!(f, "IGNORED {:?}", metadata),
                    BoltSummary::Failure(ref metadata) => write!(f, "FAILURE {:?}", metadata),
                }
            },
            None => write!(f, "None"),
        }
    }
}

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
            use $crate::packstream::{Value, ValueCast};

            let mut map : HashMap<&str, Value> = HashMap::new();
            $(
                map.insert($key, ValueCast::from(&$value));
            )+;

            map
        }
    };
);


// GRAPH //

pub trait Bolt {
    fn protocol_version(&self) -> u32;
    fn server_version(&self) -> &str;
    fn begin(&mut self, bookmark: Option<String>);
    fn commit(&mut self) -> CommitResult;
    fn reset(&mut self);
    fn rollback(&mut self);
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> Cursor;
    fn send(&mut self);

    /// Fetch the header summary
    fn fetch_header(&mut self, cursor: Cursor) -> Option<BoltSummary>;

    /// Fetch some detail, if available
    fn fetch_detail(&mut self, cursor: Cursor) -> Option<BoltDetail>;

    /// Fetch the footer summary
    fn fetch_footer(&mut self, cursor: Cursor) -> Option<BoltSummary>;
}
impl Bolt {
    pub fn connect(address: &str, user: &str, password: &str) -> Box<Bolt> {
        Box::new(BoltConnection::new(&address[..], &user[..], &password[..]))
    }
}

struct BoltConnection {
    bolt: BoltStream,
    server_version: Option<String>,
}
impl BoltConnection {
    pub fn new(address: &str, user: &str, password: &str) -> BoltConnection {
        let mut bolt = BoltStream::connect(address);

        bolt.pack_init(user, password);
        let init = bolt.collect_response();
        bolt.send();
        let init_summary = bolt.fetch_summary(init);
        let summary = init_summary.unwrap();
        bolt.compact_responses();

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

        BoltConnection {
            bolt: bolt,
            server_version: server_version,
        }
    }
}
impl Bolt for BoltConnection {

    fn protocol_version(&self) -> u32 {
        self.bolt.protocol_version()
    }

    fn server_version(&self) -> &str {
        match self.server_version {
            Some(ref version) => &version[..],
            None => "",
        }
    }

    fn begin(&mut self, bookmark: Option<String>) {
        let _ = writeln!(stderr(), "BEGIN @{:?}", bookmark);
        self.bolt.pack_run("BEGIN", match bookmark {
            Some(string) => parameters!("bookmark" => string),
            _ => parameters!(),
        });
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
            Some(BoltSummary::Success(ref fields)) => match fields.get(0) {
                Some(&Value::Map(ref metadata)) => match metadata.get("bookmark") {
                    Some(&Value::String(ref bookmark)) => Some(bookmark.clone()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };

        let _ = writeln!(stderr(), "COMMIT @{:?}", bookmark);
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

    fn fetch_header(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        let summary = self.bolt.fetch_summary(cursor.head);
        let _ = writeln!(stderr(), "HEADER {:?}", summary);
        self.bolt.compact_responses();
        match summary {
            Some(BoltSummary::Ignored(_)) => panic!("RUN was IGNORED"),
            Some(BoltSummary::Failure(_)) => {
                self.bolt.pack_ack_failure();
                self.bolt.ignore_response();
                self.bolt.send();
            },
            _ => (),
        };
        summary
    }

    fn fetch_detail(&mut self, cursor: Cursor) -> Option<BoltDetail> {
        self.bolt.fetch_detail(cursor.body)
    }

    fn fetch_footer(&mut self, cursor: Cursor) -> Option<BoltSummary> {
        let summary = self.bolt.fetch_summary(cursor.body);
        let _ = writeln!(stderr(), "FOOTER {:?}", summary);
        self.bolt.compact_responses();
        match summary {
            Some(BoltSummary::Failure(_)) => {
                self.bolt.pack_ack_failure();
                self.bolt.ignore_response();
                self.bolt.send();
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
