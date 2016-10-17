mod packstream;

use packstream::{Value, Packer, Unpacker};

use std::vec::Vec;
use std::collections::HashMap;
use std::io::prelude::*;
use std::net::TcpStream;

const BOLT: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const RAW_BOLT_VERSIONS: [u8; 16] = [0x00, 0x00, 0x00, 0x01,
                                     0x00, 0x00, 0x00, 0x00,
                                     0x00, 0x00, 0x00, 0x00,
                                     0x00, 0x00, 0x00, 0x00];

const MAX_CHUNK_SIZE: usize = 0xFFFF;
const USER_AGENT: &'static str = "rusty-bolt/0.1.0";

macro_rules! log(
    ($($arg:tt)*) => { {
        let r = write!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

macro_rules! log_line(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

struct BoltStream {
    stream: TcpStream,
    packer: Packer,
    unpacker: Unpacker,
    request_markers: Vec<usize>,
    responses: Vec<Box<Response>>,
}

impl BoltStream {
    fn connect(address: &str) -> BoltStream {
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
                println!("Using Bolt v{}", version)
            },
            Err(e) => panic!("Got an error: {}", e),
        }

        BoltStream { stream: stream, packer: Packer::new(), unpacker: Unpacker::new(),
                     request_markers: vec!(), responses: vec!() }
    }

    fn send_all(&mut self) {
        let mut offset: usize = 0;
        for &mark in &self.request_markers {
            log!("C:");
            for chunk_data in self.packer[offset..mark].chunks(MAX_CHUNK_SIZE) {
                let chunk_size = chunk_data.len();
                let chunk_header = [(chunk_size >> 8) as u8, chunk_size as u8];
                let _ = self.stream.write(&chunk_header).unwrap();
                log!(" [{:02X} {:02X}]", chunk_header[0], chunk_header[1]);

                let _ = self.stream.write(&chunk_data).unwrap();
                for i in 0..chunk_data.len() {
                    log!(" {:02X}", chunk_data[i]);
                }
            }
            let _ = self.stream.write(&[0, 0]).unwrap();
            log_line!(" [00 00]");
            offset = mark;
        }
        self.packer.clear();
        self.request_markers.clear();
    }

    fn _fetch_chunk_size(&mut self) -> usize {
        let mut chunk_header = &mut [0u8; 2];
        let _ = self.stream.read_exact(chunk_header);
        log_line!("S: [{:02X} {:02X}]", chunk_header[0] as u8, chunk_header[1] as u8);
        0x100 * chunk_header[0] as usize + chunk_header[1] as usize
    }

    /**
     * Read the next message from the stream into the read buffer.
     */
    fn fetch(&mut self) -> u8 {
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
                        let response = self.responses.remove(0);
                        match fields[0] {  // TODO: handle not enough fields
                            Value::Map(ref metadata) => response.on_success(metadata),
                            _ => panic!("SUCCESS metadata is not a map"),
                        }
                    },
                    0x71 => {
                        let ref response = self.responses[0];
                        match fields[0] {  // TODO: handle not enough fields
                            Value::List(ref data) => response.on_record(data),
                            _ => panic!("RECORD data is not a list"),
                        }
                    },
                    0x7E => {
                        let response = self.responses.remove(0);
                        match fields[0] {  // TODO: handle not enough fields
                            Value::Map(ref metadata) => response.on_ignored(metadata),
                            _ => panic!("IGNORED metadata is not a map"),
                        }
                    },
                    0x7F => {
                        let response = self.responses.remove(0);
                        match fields[0] {  // TODO: handle not enough fields
                            Value::Map(ref metadata) => response.on_failure(metadata),
                            _ => panic!("FAILURE metadata is not a map"),
                        }
                    },
                    _ => panic!("Unknown response message with signature {:02X}", signature),
                }
                return signature;
            },
            _ => panic!("Response message is not a structure"),
        }
    }

    fn fetch_until_summary(&mut self) -> u8 {
        let mut signature = self.fetch();
        while signature != 0x70 && signature != 0x7E && signature != 0x7F {
            signature = self.fetch();
        }

        signature
    }

    fn pack_init<R: 'static + Response>(&mut self, user: &str, password: &str, response: R) {
        self.packer.pack_structure_header(2, 0x01);
        self.packer.pack_string(USER_AGENT);
        self.packer.pack_map_header(3);
        self.packer.pack_string("scheme");
        self.packer.pack_string("basic");
        self.packer.pack_string("principal");
        self.packer.pack_string(user);
        self.packer.pack_string("credentials");
        self.packer.pack_string(password);
        self.request_markers.push(self.packer.len());
        self.responses.push(Box::new(response));
    }

    fn pack_run<R: 'static + Response>(&mut self, statement: &str, parameters: HashMap<&str, Value>, response: R) {
        self.packer.pack_structure_header(2, 0x10);
        self.packer.pack_string(statement);
        self.packer.pack_map_header(0);
        //println!("{:?}", parameters);
        for (name, value) in &parameters {
            self.packer.pack_string(name);
            self.packer.pack(value);
        }
        self.request_markers.push(self.packer.len());
        self.responses.push(Box::new(response));
    }

    fn pack_pull_all<R: 'static + Response>(&mut self, response: R) {
        self.packer.pack_structure_header(0, 0x3F);
        self.request_markers.push(self.packer.len());
        self.responses.push(Box::new(response));
    }

}

trait Response {
    fn on_success(&self, metadata: &HashMap<String, Value>);
    fn on_record(&self, data: &Vec<Value>);
    fn on_ignored(&self, metadata: &HashMap<String, Value>);
    fn on_failure(&self, metadata: &HashMap<String, Value>);
}

struct DumpingResponse {
}

impl Response for DumpingResponse {
    fn on_success(&self, metadata: &HashMap<String, Value>) {
        println!("S: SUCCESS {:?}", metadata);
    }

    fn on_record(&self, data: &Vec<Value>) {
        println!("S: RECORD {:?}", data);
    }

    fn on_ignored(&self, metadata: &HashMap<String, Value>) {
        println!("S: IGNORED {:?}", metadata);
    }

    fn on_failure(&self, metadata: &HashMap<String, Value>) {
        println!("S: FAILURE {:?}", metadata);
    }
}

macro_rules! parameters(
    { $($key:expr => $value:expr),* } => {
        {
            let mut map : HashMap<&str, Value> = HashMap::new();
            $(
                map.insert($key, packstream::ValueCast::from(&$value));
            )+;
            map
        }
     };
);

fn main() {
    let mut bolt = BoltStream::connect("127.0.0.1:7687");

    bolt.pack_init("neo4j", "password", DumpingResponse{});
    bolt.send_all();
    bolt.fetch_until_summary();

    bolt.pack_run("RETURN $x, 1, 1000, 1000000, 1000000000, 1000000000000",
                  parameters!("x" => 1i64, "y" => "hello"),
                  DumpingResponse{});
    bolt.pack_pull_all(DumpingResponse{});
    bolt.send_all();
    bolt.fetch_until_summary();  // SUCCESS (RUN)
    bolt.fetch_until_summary();  // SUCCESS (PULL_ALL)

}
