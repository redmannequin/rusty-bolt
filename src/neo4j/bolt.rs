use std::fmt;
use std::vec::Vec;
use std::collections::{HashMap, VecDeque};
use std::io::prelude::*;
use std::net::{TcpStream, ToSocketAddrs};

use neo4j::packstream::{Value, Packer, Unpacker};

const BOLT: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const RAW_BOLT_VERSIONS: [u8; 16] = [0x00, 0x00, 0x00, 0x01,
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
}

impl BoltStream {
    pub fn connect<A: ToSocketAddrs>(address: A) -> BoltStream {
        let mut stream = TcpStream::connect(address).unwrap();

        let _ = stream.write(&BOLT);
        let _ = stream.write(&RAW_BOLT_VERSIONS);
        let mut buf = [0; 4];
        let result = stream.read(&mut buf);
        match result {
            Ok(_) => {
                let version: u32 = (buf[0] as u32) << 24 |
                                   (buf[1] as u32) << 16 |
                                   (buf[2] as u32) << 8 |
                                   (buf[3] as u32);
                info!("S: <VERSION {}>", version)
            },
            Err(e) => panic!("Got an error: {}", e),
        }

        BoltStream { stream: stream, packer: Packer::new(), unpacker: Unpacker::new(),
                     request_markers: VecDeque::new(), responses: VecDeque::new(),
                     responses_done: 0, current_response_index: 0 }
    }

    pub fn pack_init(&mut self, user: &str, password: &str) {
        info!("C: INIT {:?} {{\"scheme\": \"basic\", \"principal\": {:?}, \"credentials\": \"...\"}}", USER_AGENT, user);
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

    pub fn pack_ack_failure(&mut self) {
        info!("C: ACK_FAILURE");
        self.packer.pack_structure_header(0, 0x0E);
        self.request_markers.push_back(self.packer.len());
    }

    pub fn pack_reset(&mut self) {
        info!("C: RESET");
        self.packer.pack_structure_header(0, 0x0F);
        self.request_markers.push_back(self.packer.len());
    }

    pub fn pack_run(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        info!("C: RUN {:?} {:?}", statement, parameters);
        self.packer.pack_structure_header(2, 0x10);
        self.packer.pack_string(statement);
        self.packer.pack_map_header(parameters.len());
        for (name, value) in &parameters {
            self.packer.pack_string(name);
            self.packer.pack(value);
        }
        self.request_markers.push_back(self.packer.len());
    }

    pub fn pack_discard_all(&mut self) {
        info!("C: DISCARD_ALL");
        self.packer.pack_structure_header(0, 0x2F);
        self.request_markers.push_back(self.packer.len());
    }

    pub fn pack_pull_all(&mut self) {
        info!("C: PULL_ALL");
        self.packer.pack_structure_header(0, 0x3F);
        self.request_markers.push_back(self.packer.len());
    }

    /// Send all queued outgoing messages
    // TODO: autosend when response is requested (maybe)
    pub fn send(&mut self) {
        info!("C: <SEND>");
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
                        info!("S: SUCCESS {:?}", fields[0]);
                        let mut response = self.responses.get_mut(self.current_response_index).unwrap();
                        response.summary = Some(BoltSummary::Success(fields));
                        self.current_response_index += 1;
                    },
                    0x71 => {
                        info!("S: RECORD {:?}", fields[0]);
                        let mut response = self.responses.get_mut(self.current_response_index).unwrap();
                        response.detail.push_back(BoltDetail::Record(fields));
                    },
                    0x7E => {
                        info!("S: IGNORED {:?}", fields[0]);
                        let mut response = self.responses.get_mut(self.current_response_index).unwrap();
                        response.summary = Some(BoltSummary::Ignored(fields));
                        self.current_response_index += 1;
                    },
                    0x7F => {
                        info!("S: FAILURE {:?}", fields[0]);
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

#[cfg(test)]
mod test {

}
