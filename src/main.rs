use std::fmt;
use std::vec::Vec;
use std::collections::HashMap;
use std::io::prelude::*;
use std::net::TcpStream;

const BOLT: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const RAW_BOLT_VERSIONS: [u8; 16] = [0x00, 0x00, 0x00, 0x01,
                                     0x00, 0x00, 0x00, 0x00,
                                     0x00, 0x00, 0x00, 0x00,
                                     0x00, 0x00, 0x00, 0x00];

fn connect(address: &str) -> TcpStream {
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
    return stream;
}

const MAX_CHUNK_SIZE: usize = 0xFFFF;
const USER_AGENT: &'static str = "rusty-bolt/0.1.0";

//macro_rules! log(
//    ($($arg:tt)*) => { {
//        let r = write!(&mut ::std::io::stderr(), $($arg)*);
//        r.expect("failed printing to stderr");
//    } }
//);
//
//macro_rules! log_line(
//    ($($arg:tt)*) => { {
//        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
//        r.expect("failed printing to stderr");
//    } }
//);

struct BoltStream<'t> {
    stream: &'t mut TcpStream,
    read_buffer: Vec<u8>,
    read_offset: usize,
    write_buffer: [u8; MAX_CHUNK_SIZE],
    write_offset: usize,
}

impl<'t> BoltStream<'t> {
    fn new(stream: &mut TcpStream) -> BoltStream {
        BoltStream { stream: stream, read_buffer: vec![0u8; 0], read_offset: 0,
                     write_buffer: [0u8; MAX_CHUNK_SIZE], write_offset: 0 }
    }

    fn read_chunk_size(&mut self) -> usize {
        let mut chunk_header = &mut [0u8; 2];
        let _ = self.stream.read_exact(chunk_header);
//        log!("S: [{:02X} {:02X}]", chunk_header[0] as u8, chunk_header[1] as u8);
        0x100 * chunk_header[0] as usize + chunk_header[1] as usize
    }

    /**
     * Read the next message from the stream into the read buffer.
     */
    fn read_message(&mut self, response: &mut Response) {
        self.read_buffer.clear();
        self.read_offset = 0;
        let mut chunk_size: usize = self.read_chunk_size();
        while chunk_size > 0 {
            let start: usize = self.read_buffer.len();
            let end: usize = start + chunk_size;
            self.read_buffer.resize(end, 0);
            let _ = self.stream.read_exact(&mut self.read_buffer[start..end]);
//            for i in start..end {
//                log!(" {:02X}", self.read_buffer[i]);
//            }
//            log_line!("");
            chunk_size = self.read_chunk_size();
        }
//        log_line!("");

        let message: Value = self.read_value();
        match message {
            Value::Structure { signature, fields } => {
                match signature {
                    0x70 => {
                        match fields[0] {  // TODO: handle not enough fields
                            Value::Map(ref metadata) => response.on_success(metadata),
                            _ => panic!("SUCCESS metadata is not a map"),
                        }
                    },
                    0x71 => {
                        match fields[0] {  // TODO: handle not enough fields
                            Value::List(ref data) => response.on_record(data),
                            _ => panic!("RECORD data is not a list"),
                        }
                    },
                    0x7E => {
                        match fields[0] {  // TODO: handle not enough fields
                            Value::Map(ref metadata) => response.on_ignored(metadata),
                            _ => panic!("IGNORED metadata is not a map"),
                        }
                    },
                    0x7F => {
                        match fields[0] {  // TODO: handle not enough fields
                            Value::Map(ref metadata) => response.on_failure(metadata),
                            _ => panic!("FAILURE metadata is not a map"),
                        }
                    },
                    _ => panic!("Unknown response message with signature {:02X}", signature),
                }
            },
            _ => panic!("Response message is not a structure"),
        }
    }

}

impl<'t> Write for BoltStream<'t> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut read_offset: usize = 0;
        while buf.len() - read_offset > self.write_buffer.len() - self.write_offset {
            let size: usize = self.write_buffer.len() - self.write_offset;
            let write_end = self.write_offset + size;
            let read_end = read_offset + size;
            let data = &buf[read_offset..read_end];
            self.write_buffer[self.write_offset..write_end].copy_from_slice(data);
            self.write_offset = write_end;
            let _ = self.flush();
            read_offset = read_end;
        }
        let write_end: usize = self.write_offset + buf.len() - read_offset;
        let data = &buf[read_offset..];
        self.write_buffer[self.write_offset..write_end].copy_from_slice(data);
        self.write_offset = write_end;
        return Ok(0);
    }

    // write chunk to ultimate writer
    fn flush(&mut self) -> std::io::Result<()> {
        let size: usize = self.write_offset;
        let chunk_header = &[(size >> 8) as u8, size as u8];
        let _ = self.stream.write(chunk_header);
        if size > 0 {
            let chunk_data = &self.write_buffer[0..size];
            let _ = self.stream.write(chunk_data);
//            log!("C: [{:02X} {:02X}]", chunk_header[0], chunk_header[1]);
//            for i in 0..chunk_data.len() {
//                log!(" {:02X}", chunk_data[i]);
//            }
//            log_line!("");
        }
//        else {
//            log_line!("C: [{:02X} {:02X}]", chunk_header[0], chunk_header[1]);
//        }
        self.write_offset = 0;
        return Ok(());
    }
}

trait WriteData {
    fn write_null(&mut self);
    fn write_boolean(&mut self, value: bool);
    fn write_integer(&mut self, value: i64);
    fn write_string(&mut self, value: &str);
    fn write_map_header(&mut self, size: usize);
    fn write_structure_header(&mut self, size: usize, signature: u8);
}

impl<'t> WriteData for BoltStream<'t> {
    fn write_null(&mut self) {
        let _ = self.write(&[0xC0]);
    }

    fn write_boolean(&mut self, value: bool) {
        if value {
            let _ = self.write(&[0xC3]);
        }
        else {
            let _ = self.write(&[0xC2]);
        }
    }

    fn write_integer(&mut self, value: i64) {
        if -0x10 <= value && value < 0x80 {
            // TINY_INT
            let _ = self.write(&[value as u8]);
        }
        else if -0x80 <= value && value < 0x80 {
            // INT_8
            let _ = self.write(&[0xC8, value as u8]);
        }
        else if -0x8000 <= value && value < 0x8000 {
            // INT_16
            let _ = self.write(&[0xC9, (value >> 8) as u8,
                                        value       as u8]);
        }
        else if -0x80000000 <= value && value < 0x80000000 {
            // INT_32
            let _ = self.write(&[0xCA, (value >> 24) as u8,
                                       (value >> 16) as u8,
                                       (value >> 8)  as u8,
                                        value        as u8]);
        }
        else {
            // INT_64
            let _ = self.write(&[0xCB, (value >> 56) as u8,
                                       (value >> 48) as u8,
                                       (value >> 40) as u8,
                                       (value >> 32) as u8,
                                       (value >> 24) as u8,
                                       (value >> 16) as u8,
                                       (value >> 8)  as u8,
                                        value        as u8]);
        }
    }

    fn write_string(&mut self, value: &str) {
        let size: usize = value.len();
        if size < 0x10 {
            let _ = self.write(&[0x80 + size as u8]);
        }
        else if size < 0x100 {
            let _ = self.write(&[0xD0, size as u8]);
        }
        else if size < 0x10000 {
            let _ = self.write(&[0xD1, (size >> 8) as u8, size as u8]);
        }
        else if size < 0x100000000 {
            let _ = self.write(&[0xD2, (size >> 24) as u8, (size >> 16) as u8,
                                       (size >> 8) as u8, size as u8]);
        }
        else {
            panic!("String too long to pack");
        }
        let _ = self.write(value.as_bytes());
    }

    fn write_map_header(&mut self, size: usize) {
        if size < 0x10 {
            let _ = self.write(&[0xA0 + size as u8]);
        }
        else if size < 0x100 {
            let _ = self.write(&[0xD8, size as u8]);
        }
        else if size < 0x10000 {
            let _ = self.write(&[0xD9, (size >> 8) as u8, size as u8]);
        }
        else if size < 0x100000000 {
            let _ = self.write(&[0xDA, (size >> 24) as u8, (size >> 16) as u8,
                                       (size >> 8) as u8, size as u8]);
        }
        else {
            panic!("Map too big to pack");
        }
    }

    fn write_structure_header(&mut self, size: usize, signature: u8) {
        if size < 0x10 {
            let _ = self.write(&[0xB0 + size as u8, signature]);
        }
        else if size < 0x100 {
            let _ = self.write(&[0xDC, size as u8, signature]);
        }
        else if size < 0x10000 {
            let _ = self.write(&[0xDD, (size >> 8) as u8, size as u8, signature]);
        }
        else {
            panic!("Structure too big to pack");
        }
    }

}

trait ReadData {
    fn read_value(&mut self) -> Value;
    fn read_u8(&mut self) -> u8;
    fn read_string(&mut self, size: usize) -> Value;
    fn read_list(&mut self, size: usize) -> Value;
    fn read_map(&mut self, size: usize) -> Value;
    fn read_structure(&mut self, size: usize) -> Value;
}

impl<'t> ReadData for BoltStream<'t> {

    fn read_value(&mut self) -> Value {
        let marker = self.read_u8();
        match marker {
            0x00...0x7F => Value::Integer(marker as i64),
            0x80...0x8F => self.read_string((marker & 0x0F) as usize),
            0x90...0x9F => self.read_list((marker & 0x0F) as usize),
            0xA0...0xAF => self.read_map((marker & 0x0F) as usize),
            0xB0...0xBF => self.read_structure((marker & 0x0F) as usize),
            0xC0 => Value::Null,
            // TODO: C1
            0xC2 => Value::Boolean(false),
            0xC3 => Value::Boolean(true),
            0xD0 => {
                let size: usize = self.read_u8() as usize;
                self.read_string(size)
            },
            0xF0...0xFF => Value::Integer(marker as i64 - 0x100),
            _ => panic!("Illegal value with marker {:02X}", marker),
        }
    }

    fn read_u8(&mut self) -> u8 {
        let value: u8 = self.read_buffer[self.read_offset];
        self.read_offset += 1;
        value
    }

    fn read_string(&mut self, size: usize) -> Value {
        let end_offset = self.read_offset + size;
        let value = String::from_utf8_lossy(&self.read_buffer[self.read_offset..end_offset]).into_owned();
        self.read_offset = end_offset;
        Value::String(value)
    }

    fn read_list(&mut self, size: usize) -> Value {
        let mut value = Vec::with_capacity(size);
        for _ in 0..size {
            value.push(self.read_value());
        }
        Value::List(value)
    }

    fn read_map(&mut self, size: usize) -> Value {
        let mut value = HashMap::with_capacity(size);
        for _ in 0..size {
            let key = self.read_value();
            match key {
                Value::String(k) => {
                    value.insert(k, self.read_value());
                },
                _ => panic!("Key is not a string"),
            }
        }
        Value::Map(value)
    }

    fn read_structure(&mut self, size: usize) -> Value {
        let signature: u8 = self.read_u8();
        let mut fields: Vec<Value> = vec!();
        for _ in 0..size {
            fields.push(self.read_value());
        }
        Value::Structure { signature: signature, fields: fields }
    }

}

trait WriteMessage {
    fn write_init(&mut self, user: &str, password: &str);
    fn write_run(&mut self, statement: &str);
    fn write_pull_all(&mut self);
}

impl<'t> WriteMessage for BoltStream<'t> {

    fn write_init(&mut self, user: &str, password: &str) {
        self.write_structure_header(2, 0x01);
        self.write_string(USER_AGENT);
        self.write_map_header(3);
        self.write_string("scheme");
        self.write_string("basic");
        self.write_string("principal");
        self.write_string(user);
        self.write_string("credentials");
        self.write_string(password);
        let _ = self.flush();
        let _ = self.flush();
    }

    fn write_run(&mut self, statement: &str) {
        self.write_structure_header(2, 0x10);
        self.write_string(statement);
        self.write_map_header(0);
        let _ = self.flush();
        let _ = self.flush();
    }

    fn write_pull_all(&mut self) {
        self.write_structure_header(0, 0x3F);
        let _ = self.flush();
        let _ = self.flush();
    }

}

enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Structure { signature: u8, fields: Vec<Value> },
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Value::Null => write!(f, "null"),
            &Value::Boolean(ref value) => write!(f, "{:?}", value),
            &Value::Integer(ref value) => write!(f, "{:?}", value),
            &Value::String(ref value) => write!(f, "{:?}", value),
            // TODO
            _ => write!(f, "?"),
        }
    }
}

trait Response {
    fn on_success(&mut self, metadata: &HashMap<String, Value>);
    fn on_record(&mut self, data: &Vec<Value>);
    fn on_ignored(&mut self, metadata: &HashMap<String, Value>);
    fn on_failure(&mut self, metadata: &HashMap<String, Value>);
}

struct DumpingResponse {
}

impl Response for DumpingResponse {
    fn on_success(&mut self, metadata: &HashMap<String, Value>) {
        println!("S: SUCCESS {:?}", metadata);
    }

    fn on_record(&mut self, data: &Vec<Value>) {
        println!("S: RECORD {:?}", data);
    }

    fn on_ignored(&mut self, metadata: &HashMap<String, Value>) {
        println!("S: IGNORED {:?}", metadata);
    }

    fn on_failure(&mut self, metadata: &HashMap<String, Value>) {
        println!("S: FAILURE {:?}", metadata);
    }
}

fn main() {
    let mut out = connect("127.0.0.1:7687");
    //let mut out = std::io::stdout();
    let mut bolt = BoltStream::new(&mut out);
    let mut response = &mut DumpingResponse {};

    bolt.write_init("neo4j", "password");
    bolt.read_message(response);

    bolt.write_run("UNWIND range(1, 3) AS n RETURN n");
    bolt.write_pull_all();
    bolt.read_message(response);  // SUCCESS (RUN)
    bolt.read_message(response);  // RECORD
    bolt.read_message(response);  // RECORD
    bolt.read_message(response);  // RECORD
    bolt.read_message(response);  // SUCCESS (PULL_ALL)

}
