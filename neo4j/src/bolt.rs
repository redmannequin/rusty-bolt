use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    io::{self, prelude::*},
    net::{TcpStream, ToSocketAddrs},
    result,
};

use packstream::{parameters, Data, Value};

use byteorder::{BigEndian, ReadBytesExt};
use log::debug;

use crate::chunk::ChunkStream;

const HANDSHAKE: [u8; 20] = [
    0x60, 0x60, 0xB0, 0x17,
    0x00, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
];

mod sig {
    pub const INIT: u8 = 0x01;
    pub const RUN: u8 = 0x10;
    pub const DISCARD_ALL: u8 = 0x2F;
    pub const PULL_ALL: u8 = 0x3F;
    pub const ACK_FAILURE: u8 = 0x0E;
    pub const RESET: u8 = 0x0F;
    pub const RECORD: u8 = 0x71;
    pub const SUCCESS: u8 = 0x70;
    pub const FAILURE: u8 = 0x7F;
    pub const IGNORED: u8 = 0x7E;
}

#[derive(Debug)]
pub enum BoltError {
    Connect(String),
    Handshake(String),
    Socket(io::Error),
}

impl fmt::Display for BoltError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BoltError::Connect(ref err) => write!(f, "Connect error: {}", err),
            BoltError::Handshake(ref err) => write!(f, "Handshake error: {}", err),
            BoltError::Socket(ref err) => write!(f, "Socket error: {}", err),
        }
    }
}

impl Error for BoltError {
    fn description(&self) -> &str {
        match *self {
            BoltError::Connect(ref err) => err,
            BoltError::Handshake(ref err) => err,
            BoltError::Socket(ref err) => err.description(),
        }
    }
}

impl From<io::Error> for BoltError {
    fn from(val: io::Error) -> Self {
        BoltError::Socket(val)
    }
}

pub struct BoltStream {
    stream: ChunkStream<TcpStream>,
    requests: Vec<Vec<u8>>,
    responses: VecDeque<BoltResponse>,
    responses_done: usize,
    current_response_index: usize,
    protocol_version: u32,
}

pub type Result<T> = result::Result<T, BoltError>;

impl BoltStream {
    pub fn connect<A: ToSocketAddrs>(address: A) -> Result<BoltStream> {
        match TcpStream::connect(address) {
            Ok(mut stream) => match stream.write(&HANDSHAKE) {
                Ok(_) => match stream.read_u32::<BigEndian>() {
                    Ok(protocol_version) => {
                        debug!("S: <VERSION {}>", protocol_version);
                        Ok(BoltStream {
                            stream: ChunkStream::new(stream),
                            requests: Vec::new(),
                            responses: VecDeque::new(),
                            responses_done: 0,
                            current_response_index: 0,
                            protocol_version,
                        })
                    }
                    Err(_) => Err(BoltError::Handshake(String::from("Error on read"))),
                },
                Err(_) => Err(BoltError::Handshake(String::from("Error on write"))),
            },
            Err(err) => Err(BoltError::Connect(format!("Error on connect: {:?}", err))),
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Pack an INIT message.
    ///
    pub fn init(&mut self, user_agent: &str, user: &str, password: &str) {
        debug!(
            "C: INIT {:?} {{\"scheme\": \"basic\", \"principal\": {:?}, \"credentials\": \"...\"}}",
            user_agent, user
        );
        self.requests.push(
            Value::Structure {
                signature: sig::INIT,
                fields: vec![
                    user_agent.into(),
                    parameters!(
                        "scheme" => "basic",
                        "principal" => user,
                        "credentials" => password
                    )
                    .into(),
                ],
            }
            .pack_into()
            .unwrap(),
        );
    }

    /// Pack an ACK_FAILURE message.
    ///
    pub fn ack_failure(&mut self) {
        debug!("C: ACK_FAILURE");
        self.requests.push(
            Value::Structure {
                signature: sig::ACK_FAILURE,
                fields: vec![],
            }
            .pack_into()
            .unwrap(),
        );
    }

    /// Pack a RESET message.
    ///
    pub fn reset(&mut self) {
        debug!("C: RESET");
        self.requests.push(
            Value::Structure {
                signature: sig::RESET,
                fields: vec![],
            }
            .pack_into()
            .unwrap(),
        );
    }

    /// Pack a RUN message.
    ///
    pub fn run(&mut self, statement: &str, parameters: Option<Value>) {
        debug!("C: RUN {:?} {:?}", statement, parameters);
        self.requests.push(
            Value::Structure {
                signature: sig::RUN,
                fields: vec![
                    statement.into(),
                    parameters.unwrap_or_else(|| Value::Map(HashMap::new())),
                ],
            }
            .pack_into()
            .unwrap(),
        );
    }

    /// Pack a DISCARD_ALL message.
    ///
    pub fn discard_all(&mut self) {
        debug!("C: DISCARD_ALL");
        self.requests.push(
            Value::Structure {
                signature: sig::DISCARD_ALL,
                fields: vec![],
            }
            .pack_into()
            .unwrap(),
        );
    }

    /// Pack a PULL_ALL message.
    ///
    pub fn pull_all(&mut self) {
        debug!("C: PULL_ALL");
        self.requests.push(
            Value::Structure {
                signature: sig::PULL_ALL,
                fields: vec![],
            }
            .pack_into()
            .unwrap(),
        );
    }

    /// Send all queued outgoing messages.
    ///
    pub fn send(&mut self) {
        debug!("C: <SEND>");
        for req in self.requests.drain(..) {
            self.stream.send(&req[..]).unwrap();
        }
    }

    pub fn collect_response(&mut self) -> usize {
        self.responses.push_back(BoltResponse::new());
        self.responses_done + self.responses.len() - 1
    }

    pub fn ignore_response(&mut self) {
        self.responses.push_back(BoltResponse::done());
    }

    pub fn compact_responses(&mut self) {
        let mut pruning = true;
        while pruning && self.current_response_index > 0 {
            match self.responses.front() {
                Some(response) => {
                    if !response.done {
                        pruning = false;
                    }
                }
                _ => {
                    pruning = false;
                }
            }
            if pruning {
                self.responses.pop_front();
                self.responses_done += 1;
                self.current_response_index -= 1;
            }
        }
    }

    /// Fetches the last message to fail before a given ignored message.
    /// Returns None if a failure cannot be found inside the current buffer.
    ///
    pub fn fetch_failure(&mut self, response_id: usize) -> Option<BoltSummary> {
        let response_index = response_id - self.responses_done;
        let from_end = self.responses.len() - response_index;
        let mut iter = self
            .responses
            .iter_mut()
            .rev()
            .skip(from_end)
            .skip_while(|r| match r.summary {
                Some(ref s) => match *s {
                    BoltSummary::Failure(_) => false,
                    _ => true,
                },
                None => true,
            });
        match iter.next() {
            Some(r) => r.summary.take(),
            None => None,
        }
    }

    /// Fetches the next response message for the designated response,
    /// assuming that response is not already completely buffered.
    ///
    pub fn fetch_record(&mut self, response_id: usize) -> Option<Data> {
        let response_index = response_id - self.responses_done;
        while self.current_response_index < response_index {
            self.fetch();
        }
        if self.current_response_index == response_index {
            self.fetch();
        }
        self.responses[response_index].detail.pop_front()
    }

    /// Fetches all response messages for the designated response,
    /// assuming that response is not already completely buffered.
    ///
    pub fn fetch_summary(&mut self, response_id: usize) -> Option<BoltSummary> {
        let response_index = response_id - self.responses_done;
        while self.current_response_index <= response_index {
            self.fetch();
        }
        let response = &mut self.responses[response_index];
        response.done = true;
        response.summary.take()
    }

    fn receive(&mut self) -> Value {
        Value::unpack(&mut &self.stream.recv().unwrap()[..]).unwrap()
    }

    /// Reads the next message from the stream into the read buffer.
    ///
    fn fetch(&mut self) {
        let msg = self.receive();
        let response = &mut self.responses[self.current_response_index];
        match msg {
            Value::Structure {
                signature,
                mut fields,
            } => match signature {
                sig::SUCCESS => {
                    self.current_response_index += 1;
                    if fields.is_empty() {
                        debug!("S: SUCCESS {{}}");
                        response.summary = Some(BoltSummary::Success(HashMap::new()));
                    } else {
                        match fields.remove(0) {
                            Value::Map(metadata) => {
                                debug!("S: SUCCESS({:?})", metadata);
                                response.summary = Some(BoltSummary::Success(metadata));
                            }
                            _ => panic!("Non-map metadata"),
                        }
                    }
                }
                sig::RECORD => {
                    if fields.is_empty() {
                        debug!("S: RECORD {{}}");
                        response.detail.push_back(Data::Record(Vec::new()));
                    } else {
                        match fields.remove(0) {
                            Value::List(data) => {
                                debug!("S: RECORD {:?}", data);
                                response.detail.push_back(Data::Record(data));
                            }
                            _ => panic!("Non-list data"),
                        }
                    }
                }
                sig::IGNORED => {
                    self.current_response_index += 1;
                    if fields.is_empty() {
                        debug!("S: IGNORED {{}}");
                        response.summary = Some(BoltSummary::Ignored(HashMap::new()));
                    } else {
                        match fields.remove(0) {
                            Value::Map(metadata) => {
                                debug!("S: IGNORED({:?})", metadata);
                                response.summary = Some(BoltSummary::Ignored(metadata));
                            }
                            _ => panic!("Non-map metadata"),
                        }
                    }
                }
                sig::FAILURE => {
                    self.current_response_index += 1;
                    if fields.is_empty() {
                        debug!("S: FAILURE {{}}");
                        response.summary = Some(BoltSummary::Failure(HashMap::new()));
                    } else {
                        match fields.remove(0) {
                            Value::Map(metadata) => {
                                debug!("S: FAILURE({:?})", metadata);
                                response.summary = Some(BoltSummary::Failure(metadata));
                            }
                            _ => panic!("Non-map metadata"),
                        }
                    }
                }
                _ => panic!("Unknown response message with signature {:02X}", signature),
            },
            _ => panic!("Response message is not a data or a summary"),
        }
    }
}

#[derive(Clone)]
pub enum BoltSummary {
    Success(HashMap<String, Value>),
    Ignored(HashMap<String, Value>),
    Failure(HashMap<String, Value>),
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

#[derive(Clone, Default)]
pub struct BoltResponse {
    detail: VecDeque<Data>,
    summary: Option<BoltSummary>,
    done: bool,
}

impl BoltResponse {
    pub fn new() -> BoltResponse {
        BoltResponse {
            detail: VecDeque::new(),
            summary: None,
            done: false,
        }
    }
    pub fn done() -> BoltResponse {
        BoltResponse {
            detail: VecDeque::new(),
            summary: None,
            done: true,
        }
    }
}

impl fmt::Debug for BoltResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.summary {
            Some(ref summary) => match *summary {
                BoltSummary::Success(ref metadata) => write!(f, "SUCCESS {:?}", metadata),
                BoltSummary::Ignored(ref metadata) => write!(f, "IGNORED {:?}", metadata),
                BoltSummary::Failure(ref metadata) => write!(f, "FAILURE {:?}", metadata),
            },
            None => write!(f, "None"),
        }
    }
}
