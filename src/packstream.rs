use std::fmt;
use std::ops::{Index, Range, RangeTo, RangeFrom, RangeFull};
use std::vec::Vec;
use std::collections::HashMap;

#[derive(PartialEq)]
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
        match *self {
            Value::Null => write!(f, "null"),
            Value::Boolean(ref value) => write!(f, "{:?}", value),
            Value::Integer(ref value) => write!(f, "{:?}", value),
            Value::String(ref value) => write!(f, "{:?}", value),
//            &Value::List(ref values) => write!(f, "[{:?}]", values.connect(", ")),
            // TODO
            _ => write!(f, "?"),
        }
    }
}

pub trait ValueCast {
    fn from(&self) -> Value;
}

impl ValueCast for bool {
    fn from(&self) -> Value {
        Value::Boolean(*self)
    }
}

impl ValueCast for char {
    fn from(&self) -> Value {
        let mut s = String::with_capacity(4);
        s.push(*self);
        Value::String(s)
    }
}

impl ValueCast for i8 {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for i16 {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for i32 {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for i64 {
    fn from(&self) -> Value {
        Value::Integer(*self)
    }
}

impl ValueCast for isize {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for u8 {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for u16 {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for u32 {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for u64 {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for usize {
    fn from(&self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl ValueCast for f32 {
    fn from(&self) -> Value {
        Value::Float(*self as f64)
    }
}

impl ValueCast for f64 {
    fn from(&self) -> Value {
        Value::Float(*self)
    }
}

impl ValueCast for &'static str {
    fn from(&self) -> Value {
        let mut s = String::with_capacity(self.len());
        s.push_str(&self);
        Value::String(s)
    }
}

pub trait ValueMatch {
    fn is_null(&self) -> bool;
    fn is_boolean(&self) -> bool;
    fn is_integer(&self) -> bool;
    fn is_float(&self) -> bool;
    fn is_string(&self) -> bool;
    fn is_list(&self) -> bool;
    fn is_map(&self) -> bool;
    fn is_structure(&self) -> bool;
}

impl ValueMatch for Value {

    fn is_null(&self) -> bool {
        match *self {
            Value::Null => true,
            _ => false
        }
    }

    fn is_boolean(&self) -> bool {
        match *self {
            Value::Boolean(_) => true,
            _ => false
        }
    }

    fn is_integer(&self) -> bool {
        match *self {
            Value::Integer(_) => true,
            _ => false
        }
    }

    fn is_float(&self) -> bool {
        match *self {
            Value::Float(_) => true,
            _ => false
        }
    }

    fn is_string(&self) -> bool {
        match *self {
            Value::String(_) => true,
            _ => false
        }
    }

    fn is_list(&self) -> bool {
        match *self {
            Value::List(_) => true,
            _ => false
        }
    }

    fn is_map(&self) -> bool {
        match *self {
            Value::Map(_) => true,
            _ => false
        }
    }

    fn is_structure(&self) -> bool {
        match *self {
            Value::Structure { signature: _, fields: _ } => true,
            _ => false
        }
    }

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

    pub fn pack(&mut self, value: &Value) {
        match value {
            &Value::Null => self.pack_null(),
            &Value::Boolean(ref x) => self.pack_boolean(*x),
            &Value::Integer(ref x) => self.pack_integer(*x),
            &Value::Float(ref x) => self.pack_float(*x),
            &Value::String(ref x) => self.pack_string(&x[..]),
            &Value::List(ref x) => {
                self.pack_list_header(x.len());
                for item in x {
                    self.pack(item);
                }
            },
            _ => panic!("Unpackable"),
        }
    }

    pub fn pack_null(&mut self) {
        let _ = self.write_slice(&[0xC0]);
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

    pub fn pack_float(&mut self, value: f64) {
        // TODO
    }

    pub fn pack_string(&mut self, value: &str) {
        let size: usize = value.len();
        if size < 0x10 {
            let _ = self.write_slice(&[0x80 + size as u8]);
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

    pub fn pack_list_header(&mut self, size: usize) {
        if size < 0x10 {
            let _ = self.write_slice(&[0x90 + size as u8]);
        }
        else if size < 0x100 {
            let _ = self.write_slice(&[0xD4, size as u8]);
        }
        else if size < 0x10000 {
            let _ = self.write_slice(&[0xD5, (size >> 8) as u8, size as u8]);
        }
        else if size < 0x100000000 {
            let _ = self.write_slice(&[0xD6, (size >> 24) as u8, (size >> 16) as u8,
                                             (size >> 8) as u8, size as u8]);
        }
        else {
            panic!("List too big to pack");
        }
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

    fn write_slice(&mut self, buf: &[u8]) {
        let start: usize = self.buffer.len();
        let end: usize = start + buf.len();
        self.buffer.resize(end, 0);
        &mut self.buffer[start..end].copy_from_slice(&buf[..]);
    }

}

impl Index<usize> for Packer {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.buffer[index]
    }
}

impl Index<Range<usize>> for Packer {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        &self.buffer[index.start..index.end]
    }
}

impl Index<RangeTo<usize>> for Packer {
    type Output = [u8];

    fn index(&self, index: RangeTo<usize>) -> &Self::Output {
        &self.buffer[..index.end]
    }
}

impl Index<RangeFrom<usize>> for Packer {
    type Output = [u8];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        &self.buffer[index.start..]
    }
}

impl Index<RangeFull> for Packer {
    type Output = [u8];

    fn index(&self, _: RangeFull) -> &[u8] {
        &self.buffer[..]
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

    pub fn from_slice(src: &[u8]) -> Unpacker {
        Unpacker { buffer: src.to_vec(), unpack_ptr: 0 }
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
            0xC8 => Value::Integer(self.unpack_i8() as i64),
            0xC9 => Value::Integer(self.unpack_i16() as i64),
            0xCA => Value::Integer(self.unpack_i32() as i64),
            0xCB => Value::Integer(self.unpack_i64() as i64),
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

    fn unpack_i8(&mut self) -> i8 {
        let value: i8 = self.buffer[self.unpack_ptr] as i8;
        self.unpack_ptr += 1;
        value
    }

    fn unpack_i16(&mut self) -> i16 {
        (self.unpack_i8() as i16) << 8 | self.unpack_u8() as i16
    }

    fn unpack_i32(&mut self) -> i32 {
        (self.unpack_i8() as i32) << 24 |
        (self.unpack_u8() as i32) << 16 |
        (self.unpack_u8() as i32) << 8 |
         self.unpack_u8() as i32
    }

    fn unpack_i64(&mut self) -> i64 {
        (self.unpack_i8() as i64) << 56 |
        (self.unpack_u8() as i64) << 48 |
        (self.unpack_u8() as i64) << 40 |
        (self.unpack_u8() as i64) << 32 |
        (self.unpack_u8() as i64) << 24 |
        (self.unpack_u8() as i64) << 16 |
        (self.unpack_u8() as i64) << 8 |
         self.unpack_u8() as i64
    }

}

#[cfg(test)]
mod tests {
    use packstream::{Value, ValueCast, ValueMatch, Packer, Unpacker};

    #[test]
    fn should_cast_value_from_true() {
        // Given
        let value = ValueCast::from(&true);

        // Then
        assert!(ValueMatch::is_boolean(&value));
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn should_cast_value_from_false() {
        // Given
        let value = ValueCast::from(&false);

        // Then
        assert!(ValueMatch::is_boolean(&value));
        assert_eq!(value, Value::Boolean(false));
    }

    #[test]
    fn should_cast_value_from_i8() {
        for i in -0x80..0x80 {
            // Given
            let value = ValueCast::from(&(i as i8));

            // Then
            assert!(ValueMatch::is_integer(&value));
            assert_eq!(value, Value::Integer(i as i64));
        }
    }

    #[test]
    fn should_cast_value_from_i16() {
        for i in -0x8000..0x8000 {
            // Given
            let value = ValueCast::from(&(i as i16));

            // Then
            assert!(ValueMatch::is_integer(&value));
            assert_eq!(value, Value::Integer(i as i64));
        }
    }

    #[test]
    fn should_cast_value_from_u8() {
        for i in 0..0x100 {
            // Given
            let value = ValueCast::from(&(i as u8));

            // Then
            assert!(ValueMatch::is_integer(&value));
            assert_eq!(value, Value::Integer(i as i64));
        }
    }

    #[test]
    fn should_cast_value_from_u16() {
        for i in 0..0x10000 {
            // Given
            let value = ValueCast::from(&(i as u16));

            // Then
            assert!(ValueMatch::is_integer(&value));
            assert_eq!(value, Value::Integer(i as i64));
        }
    }

    #[test]
    fn should_pack_and_unpack_null() {
        // Given
        let mut packer = Packer::new();

        // When
        packer.pack_null();

        // Then
        assert_eq!(&packer[..], &[0xC0]);

        // And given
        let mut unpacker = Unpacker::from_slice(&packer[..]);

        // When
        let value = unpacker.unpack();

        // Then
        assert!(ValueMatch::is_null(&value));
    }

    #[test]
    fn should_pack_and_unpack_tiny_integer() {
        for i in 0..128 {
            // Given
            let mut packer = Packer::new();

            // When
            packer.pack_integer(i as i64);

            // Then
            assert_eq!(&packer[..], &[i as u8]);

            // And given
            let mut unpacker = Unpacker::from_slice(&packer[..]);

            // When
            let value = unpacker.unpack();

            // Then
            assert!(ValueMatch::is_integer(&value));
            assert_eq!(value, Value::Integer(i as i64));
        }
    }

}
