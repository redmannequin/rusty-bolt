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
                     request_markers: VecDeque::new(), responses: VecDeque::new() }
    }

    /// Send all queued outgoing messages
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

    fn _fetch_chunk_size(&mut self) -> usize {
        let mut chunk_header = &mut [0u8; 2];
        let _ = self.stream.read_exact(chunk_header);
//        log_line!("S: [{:02X} {:02X}]", chunk_header[0] as u8, chunk_header[1] as u8);
        0x100 * chunk_header[0] as usize + chunk_header[1] as usize
    }

    /// Read the next message from the stream into the read buffer.
    pub fn fetch(&mut self) -> u8 {
        self.unpacker.clear();
        let mut chunk_size: usize = self._fetch_chunk_size();
        while chunk_size > 0 {
            let _ = self.stream.read_exact(&mut self.unpacker.buffer(chunk_size));
            chunk_size = self._fetch_chunk_size();
        }

        let message: Value = self.unpacker.unpack();
        match message {
            Value::Structure { signature, fields } => {
                match signature {
                    0x70 => {
                        info!("S: SUCCESS {:?}", fields[0]);
                        let mut response = self.responses.pop_front().unwrap();
                        response.summary = Some(BoltSummary::Success(fields));
                    },
                    0x71 => {
                        info!("S: RECORD {:?}", fields[0]);
                        let ref mut response = self.responses.front_mut().unwrap();
                        response.detail.push(BoltDetail::Record(fields));
                    },
                    0x7E => {
                        info!("S: IGNORED {:?}", fields[0]);
                        let mut response = self.responses.pop_front().unwrap();
                        response.summary = Some(BoltSummary::Ignored(fields));
                    },
                    0x7F => {
                        info!("S: FAILURE {:?}", fields[0]);
                        let mut response = self.responses.pop_front().unwrap();
                        response.summary = Some(BoltSummary::Failure(fields));
                        self.pack_ack_failure();
                    },
                    _ => panic!("Unknown response message with signature {:02X}", signature),
                }
                return signature;
            },
            _ => panic!("Response message is not a structure"),
        }
    }

    pub fn fetch_response(&mut self) -> u8 {
        let mut signature = self.fetch();
        while signature != 0x70 && signature != 0x7E && signature != 0x7F {
            signature = self.fetch();
        }
        signature
    }

    pub fn sync(&mut self) {
        self.send();
        while !self.responses.is_empty() {
            self.fetch_response();
        }
    }

    pub fn pack_init(&mut self, user: &str, password: &str) -> &BoltResponse {
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
        self.responses.push_back(BoltResponse::new());
        self.responses.back().unwrap()
    }

    pub fn pack_ack_failure(&mut self) -> &BoltResponse {
        info!("C: ACK_FAILURE");
        self.packer.pack_structure_header(0, 0x0E);
        self.request_markers.push_back(self.packer.len());
        self.responses.push_back(BoltResponse::new());
        self.responses.back().unwrap()
    }

    pub fn pack_reset(&mut self) -> &BoltResponse {
        info!("C: RESET");
        self.packer.pack_structure_header(0, 0x0F);
        self.request_markers.push_back(self.packer.len());
        self.responses.push_back(BoltResponse::new());
        self.responses.back().unwrap()
    }

    pub fn pack_run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> &BoltResponse {
        info!("C: RUN {:?} {:?}", statement, parameters);
        self.packer.pack_structure_header(2, 0x10);
        self.packer.pack_string(statement);
        self.packer.pack_map_header(parameters.len());
        for (name, value) in &parameters {
            self.packer.pack_string(name);
            self.packer.pack(value);
        }
        self.request_markers.push_back(self.packer.len());
        self.responses.push_back(BoltResponse::new());
        self.responses.back().unwrap()
    }

    pub fn pack_discard_all(&mut self) -> &BoltResponse {
        info!("C: DISCARD_ALL");
        self.packer.pack_structure_header(0, 0x2F);
        self.request_markers.push_back(self.packer.len());
        self.responses.push_back(BoltResponse::new());
        self.responses.back().unwrap()
    }

    pub fn pack_pull_all(&mut self) -> &BoltResponse {
        info!("C: PULL_ALL");
        self.packer.pack_structure_header(0, 0x3F);
        self.request_markers.push_back(self.packer.len());
        self.responses.push_back(BoltResponse::new());
        self.responses.back().unwrap()
    }

}

pub enum BoltDetail {
    Record(Vec<Value>),
}

pub enum BoltSummary {
    Success(Vec<Value>),
    Ignored(Vec<Value>),
    Failure(Vec<Value>),
}

pub struct BoltResponse {
    detail: Vec<BoltDetail>,
    summary: Option<BoltSummary>,
}
impl BoltResponse {
    pub fn new() -> BoltResponse {
        BoltResponse { detail: vec!(), summary: None }
    }
}
//
//pub trait BoltResponseHandler {
//    fn handle(&mut self, response: BoltResponse);
//}
//struct AckFailureResponseHandler;
//impl BoltResponseHandler for AckFailureResponseHandler {
//    fn handle(&mut self, response: BoltResponse) {
//        match response {
//            BoltResponse::Summary(summary) => {
//                match summary {
//                    BoltSummary::Success(_) => (),
//                    _ => panic!("Wrong type of thing!"),
//                }
//            }
//            _ => panic!("oops")
//        }
//    }
//}

#[cfg(test)]
mod test {

}
