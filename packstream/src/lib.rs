extern crate byteorder;

use std::io::{self, Cursor, Read, Write};
use std::ops::{Index, Range, RangeTo, RangeFrom, RangeFull};
use std::vec::Vec;
use std::collections::HashMap;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub mod values;
use values::{Value};

#[derive(Default)]
pub struct Packer {
    buffer: Vec<u8>,
}

impl Packer {
    pub fn new() -> Packer {
        Packer { buffer: Vec::with_capacity(4096) }
    }

    pub fn clear(&mut self) {
        self.buffer.clear()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn pack(&mut self, value: &Value) {
        match *value {
            Value::Null => self.pack_null(),
            Value::Boolean(ref x) => self.pack_boolean(*x),
            Value::Integer(ref x) => self.pack_integer(*x),
            Value::Float(ref x) => self.pack_float(*x),
            Value::String(ref x) => self.pack_string(&x[..]),
            Value::List(ref items) => {
                self.pack_list_header(items.len());
                for item in items {
                    self.pack(item);
                }
            },
            Value::Map(ref items) => {
                self.pack_map_header(items.len());
                for (key, value) in items {
                    self.pack_string(&key[..]);
                    self.pack(value);
                }
            },
            Value::Structure { signature, ref fields } => {
                self.pack_structure_header(fields.len(), signature);
                for field in fields {
                    self.pack(field);
                }
            },
        }
    }

    pub fn pack_null(&mut self) {
        self.buffer.write_u8(0xC0).unwrap();
    }

    pub fn pack_boolean(&mut self, value: bool) {
        if value {
            self.buffer.write_u8(0xC3).unwrap();
        } else {
            self.buffer.write_u8(0xC2).unwrap();
        }
    }

    pub fn pack_integer(&mut self, value: i64) {
        if -0x10 <= value && value < 0x80 {
            // TINY_INT
            self.buffer.write_i8(value as i8).unwrap();
        } else if -0x80 <= value && value < 0x80 {
            // INT_8
            self.buffer.write_u8(0xC8).unwrap();
            self.buffer.write_i8(value as i8).unwrap();
        } else if -0x8000 <= value && value < 0x8000 {
            // INT_16
            self.buffer.write_u8(0xC9).unwrap();
            self.buffer.write_i16::<BigEndian>(value as i16).unwrap();
        } else if -0x80000000 <= value && value < 0x80000000 {
            // INT_32
            self.buffer.write_u8(0xCA).unwrap();
            self.buffer.write_i32::<BigEndian>(value as i32).unwrap();
        } else {
            // INT_64
            self.buffer.write_u8(0xCB).unwrap();
            self.buffer.write_i64::<BigEndian>(value).unwrap();
        }
    }

    pub fn pack_float(&mut self, value: f64) {
        self.buffer.write_u8(0xC1).unwrap();
        self.buffer.write_f64::<BigEndian>(value).unwrap();
    }

    pub fn pack_string(&mut self, value: &str) {
        let size: usize = value.len();
        if size < 0x10 {
            self.buffer.write_u8(0x80 + size as u8).unwrap();
        } else if size < 0x100 {
            self.buffer.write_u8(0xD0).unwrap();
            self.buffer.write_u8(size as u8).unwrap();
        } else if size < 0x10000 {
            self.buffer.write_u8(0xD1).unwrap();
            self.buffer.write_u16::<BigEndian>(size as u16).unwrap();
        } else if size < 0x100000000 {
            self.buffer.write_u8(0xD2).unwrap();
            self.buffer.write_u32::<BigEndian>(size as u32).unwrap();
        } else {
            panic!("String too long to pack");
        }
        self.buffer.write_all(value.as_bytes()).unwrap();
    }

    pub fn pack_list_header(&mut self, size: usize) {
        if size < 0x10 {
            self.buffer.write_u8(0x90 + size as u8).unwrap();
        } else if size < 0x100 {
            self.buffer.write_u8(0xD4).unwrap();
            self.buffer.write_u8(size as u8).unwrap();
        } else if size < 0x10000 {
            self.buffer.write_u8(0xD5).unwrap();
            self.buffer.write_u16::<BigEndian>(size as u16).unwrap();
        } else if size < 0x100000000 {
            self.buffer.write_u8(0xD6).unwrap();
            self.buffer.write_u32::<BigEndian>(size as u32).unwrap();
        } else {
            panic!("List too big to pack");
        }
    }

    pub fn pack_map_header(&mut self, size: usize) {
        if size < 0x10 {
            self.buffer.write_u8(0xA0 + size as u8).unwrap();
        } else if size < 0x100 {
            self.buffer.write_u8(0xD8).unwrap();
            self.buffer.write_u8(size as u8).unwrap();
        } else if size < 0x10000 {
            self.buffer.write_u8(0xD9).unwrap();
            self.buffer.write_u16::<BigEndian>(size as u16).unwrap();
        } else if size < 0x100000000 {
            self.buffer.write_u8(0xDA).unwrap();
            self.buffer.write_u32::<BigEndian>(size as u32).unwrap();
        } else {
            panic!("Map too big to pack");
        }
    }

    pub fn pack_structure_header(&mut self, size: usize, signature: u8) {
        if size < 0x10 {
            self.buffer.write_u8(0xB0 + size as u8).unwrap();
        } else if size < 0x100 {
            self.buffer.write_u8(0xDC).unwrap();
            self.buffer.write_u8(size as u8).unwrap();
        } else if size < 0x10000 {
            self.buffer.write_u8(0xDD).unwrap();
            self.buffer.write_u16::<BigEndian>(size as u16).unwrap();
        } else {
            panic!("Structure too big to pack");
        }
        self.buffer.write_u8(signature).unwrap();
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
    cursor: Cursor<Vec<u8>>,
}

impl Unpacker {
    pub fn new() -> Unpacker {
        let buf: Vec<u8> = Vec::new();
        Unpacker { cursor: Cursor::new(buf) }
    }

    pub fn clear(&mut self) {
        self.cursor.get_mut().clear();
        self.cursor.set_position(0);
    }
  
    pub fn load<R>(&mut self, mut reader: R) -> io::Result<()> 
        where R: Read {
        reader.read_to_end(self.cursor.get_mut())?;
        Ok(())
    }

    pub fn load_n<R>(&mut self, reader: R, bytes: u64) -> io::Result<()>
        where R: Read {
        self.load(reader.take(bytes))
    }

    pub fn unpack(&mut self) -> Value {
        let marker = self.cursor.read_u8().unwrap();
        match marker {
            0x00...0x7F => Value::Integer(marker as i64),
            0x80...0x8F => self.unpack_string((marker & 0x0F) as usize),
            0x90...0x9F => self.unpack_list((marker & 0x0F) as usize),
            0xA0...0xAF => self.unpack_map((marker & 0x0F) as usize),
            0xB0...0xBF => self.unpack_structure((marker & 0x0F) as usize),
            0xC0 => Value::Null,
            0xC1 => Value::Float(self.cursor.read_f64::<BigEndian>().unwrap()),
            0xC2 => Value::Boolean(false),
            0xC3 => Value::Boolean(true),
            0xC8 => Value::Integer(self.cursor.read_i8().unwrap() as i64),
            0xC9 => Value::Integer(self.cursor.read_i16::<BigEndian>().unwrap() as i64),
            0xCA => Value::Integer(self.cursor.read_i32::<BigEndian>().unwrap() as i64),
            0xCB => Value::Integer(self.cursor.read_i64::<BigEndian>().unwrap()),
            0xD0 => {
                let size = self.cursor.read_u8().unwrap() as usize;
                self.unpack_string(size)
            },
            0xD1 => {
                let size = self.cursor.read_u16::<BigEndian>().unwrap() as usize;
                self.unpack_string(size)
            },
            0xD2 => {
                let size = self.cursor.read_u32::<BigEndian>().unwrap() as usize;
                self.unpack_string(size)
            },
            0xD4 => {
                let size = self.cursor.read_u8().unwrap() as usize;
                self.unpack_list(size)
            },
            0xD5 => {
                let size = self.cursor.read_u16::<BigEndian>().unwrap() as usize;
                self.unpack_list(size)
            },
            0xD6 => {
                let size = self.cursor.read_u32::<BigEndian>().unwrap() as usize;
                self.unpack_list(size)
            },
            0xD8 => {
                let size = self.cursor.read_u8().unwrap() as usize;
                self.unpack_map(size)
            },
            0xD9 => {
                let size = self.cursor.read_u16::<BigEndian>().unwrap() as usize;
                self.unpack_map(size)
            },
            0xDA => {
                let size = self.cursor.read_u32::<BigEndian>().unwrap() as usize;
                self.unpack_map(size)
            },
            0xDC => {
                let size = self.cursor.read_u8().unwrap() as usize;
                self.unpack_structure(size)
            },
            0xDD => {
                let size = self.cursor.read_u16::<BigEndian>().unwrap() as usize;
                self.unpack_structure(size)
            },
            0xF0...0xFF => Value::Integer(marker as i64 - 0x100),
            _ => panic!("Illegal value with marker {:02X}", marker),
        }
    }

    fn unpack_string(&mut self, size: usize) -> Value {
        let mut cur = &mut self.cursor;
        let mut s = String::new();
        cur.take(size as u64).read_to_string(&mut s).unwrap();
        Value::String(s)
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
        let signature: u8 = self.cursor.read_u8().unwrap();
        let mut fields: Vec<Value> = vec!();
        for _ in 0..size {
            fields.push(self.unpack());
        }
        Value::Structure { signature: signature, fields: fields }
    }
}

#[cfg(test)]
mod tests {

    mod casting {
        use super::super::*;

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
    }

    mod packing {
        use super::super::*;

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

}
