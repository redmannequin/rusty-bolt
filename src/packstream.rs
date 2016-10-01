use std::fmt;
use std::vec::Vec;
use std::collections::HashMap;

pub enum Value {
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

pub trait Pack {
    fn pack_null(&mut self);
    fn pack_boolean(&mut self, value: bool);
    fn pack_integer(&mut self, value: i64);
    fn pack_string(&mut self, value: &str);
    fn pack_map_header(&mut self, size: usize);
    fn pack_structure_header(&mut self, size: usize, signature: u8);
}

pub struct Packer {
    buffer: Vec<u8>,
}

impl Packer {
    pub fn new() -> Packer {
        Packer { buffer: vec![0u8; 0] }
    }

    pub fn clear(&mut self) {
        self.buffer.clear()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn get_chunk(&mut self, start: usize, end: usize) -> &[u8] {
        &self.buffer[start..end]
    }

    pub fn pack_null(&mut self) {
        let _ = self.write(0xC0);
    }

    pub fn pack_boolean(&mut self, value: bool) {
        if value {
            let _ = self.write_slice(&[0xC3]);
        }
        else {
            let _ = self.write_slice(&[0xC2]);
        }
    }

    pub fn pack_integer(&mut self, value: i64) {
        if -0x10 <= value && value < 0x80 {
            // TINY_INT
            let _ = self.write_slice(&[value as u8]);
        }
        else if -0x80 <= value && value < 0x80 {
            // INT_8
            let _ = self.write_slice(&[0xC8, value as u8]);
        }
        else if -0x8000 <= value && value < 0x8000 {
            // INT_16
            let _ = self.write_slice(&[0xC9, (value >> 8) as u8,
                                        value       as u8]);
        }
        else if -0x80000000 <= value && value < 0x80000000 {
            // INT_32
            let _ = self.write_slice(&[0xCA, (value >> 24) as u8,
                                       (value >> 16) as u8,
                                       (value >> 8)  as u8,
                                        value        as u8]);
        }
        else {
            // INT_64
            let _ = self.write_slice(&[0xCB, (value >> 56) as u8,
                                       (value >> 48) as u8,
                                       (value >> 40) as u8,
                                       (value >> 32) as u8,
                                       (value >> 24) as u8,
                                       (value >> 16) as u8,
                                       (value >> 8)  as u8,
                                        value        as u8]);
        }
    }

    pub fn pack_string(&mut self, value: &str) {
        let size: usize = value.len();
        if size < 0x10 {
            let _ = self.write(0x80 + size as u8);
        }
        else if size < 0x100 {
            let _ = self.write_slice(&[0xD0, size as u8]);
        }
        else if size < 0x10000 {
            let _ = self.write_slice(&[0xD1, (size >> 8) as u8, size as u8]);
        }
        else if size < 0x100000000 {
            let _ = self.write_slice(&[0xD2, (size >> 24) as u8, (size >> 16) as u8,
                                       (size >> 8) as u8, size as u8]);
        }
        else {
            panic!("String too long to pack");
        }
        let _ = self.write_slice(value.as_bytes());
    }

    pub fn pack_map_header(&mut self, size: usize) {
        if size < 0x10 {
            let _ = self.write_slice(&[0xA0 + size as u8]);
        }
        else if size < 0x100 {
            let _ = self.write_slice(&[0xD8, size as u8]);
        }
        else if size < 0x10000 {
            let _ = self.write_slice(&[0xD9, (size >> 8) as u8, size as u8]);
        }
        else if size < 0x100000000 {
            let _ = self.write_slice(&[0xDA, (size >> 24) as u8, (size >> 16) as u8,
                                       (size >> 8) as u8, size as u8]);
        }
        else {
            panic!("Map too big to pack");
        }
    }

    pub fn pack_structure_header(&mut self, size: usize, signature: u8) {
        if size < 0x10 {
            let _ = self.write_slice(&[0xB0 + size as u8, signature]);
        }
        else if size < 0x100 {
            let _ = self.write_slice(&[0xDC, size as u8, signature]);
        }
        else if size < 0x10000 {
            let _ = self.write_slice(&[0xDD, (size >> 8) as u8, size as u8, signature]);
        }
        else {
            panic!("Structure too big to pack");
        }
    }

    fn write(&mut self, byte: u8) {
        let start: usize = self.buffer.len();
        let end: usize = start + 1;
        self.buffer.resize(end, 0);
        self.buffer[start] = byte;
    }

    fn write_slice(&mut self, buf: &[u8]) {
        let start: usize = self.buffer.len();
        let end: usize = start + buf.len();
        self.buffer.resize(end, 0);
        &mut self.buffer[start..end].copy_from_slice(&buf[..]);
    }

}

pub struct Unpacker {
    buffer: Vec<u8>,
    unpack_ptr: usize,
}

impl Unpacker {
    pub fn new() -> Unpacker {
        Unpacker { buffer: vec![0u8; 0], unpack_ptr: 0 }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.unpack_ptr = 0;
    }

    pub fn buffer(&mut self, size: usize) -> &mut [u8] {
        let start: usize = self.buffer.len();
        let end: usize = start + size;
        self.buffer.resize(end, 0);

        &mut self.buffer[start..end]
    }

    pub fn unpack(&mut self) -> Value {
        let marker = self.unpack_u8();
        match marker {
            0x00...0x7F => Value::Integer(marker as i64),
            0x80...0x8F => self.unpack_string((marker & 0x0F) as usize),
            0x90...0x9F => self.unpack_list((marker & 0x0F) as usize),
            0xA0...0xAF => self.unpack_map((marker & 0x0F) as usize),
            0xB0...0xBF => self.unpack_structure((marker & 0x0F) as usize),
            0xC0 => Value::Null,
            // TODO: C1
            0xC2 => Value::Boolean(false),
            0xC3 => Value::Boolean(true),
            0xD0 => {
                let size: usize = self.unpack_u8() as usize;
                self.unpack_string(size)
            },
            0xF0...0xFF => Value::Integer(marker as i64 - 0x100),
            _ => panic!("Illegal value with marker {:02X}", marker),
        }
    }

    fn unpack_string(&mut self, size: usize) -> Value {
        let end_offset = self.unpack_ptr + size;
        let value = String::from_utf8_lossy(&self.buffer[self.unpack_ptr..end_offset]).into_owned();
        self.unpack_ptr = end_offset;
        Value::String(value)
    }

    fn unpack_list(&mut self, size: usize) -> Value {
        let mut value = Vec::with_capacity(size);
        for _ in 0..size {
            value.push(self.unpack());
        }
        Value::List(value)
    }

    fn unpack_map(&mut self, size: usize) -> Value {
        let mut value = HashMap::with_capacity(size);
        for _ in 0..size {
            let key = self.unpack();
            match key {
                Value::String(k) => {
                    value.insert(k, self.unpack());
                },
                _ => panic!("Key is not a string"),
            }
        }
        Value::Map(value)
    }

    fn unpack_structure(&mut self, size: usize) -> Value {
        let signature: u8 = self.unpack_u8();
        let mut fields: Vec<Value> = vec!();
        for _ in 0..size {
            fields.push(self.unpack());
        }
        Value::Structure { signature: signature, fields: fields }
    }

    fn unpack_u8(&mut self) -> u8 {
        let value: u8 = self.buffer[self.unpack_ptr];
        self.unpack_ptr += 1;
        value
    }

}