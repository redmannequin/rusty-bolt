use std::io::{self, Write};
use std::collections::HashMap;

use byteorder::{BigEndian, WriteBytesExt};

use super::Value;

pub type PackResult = Result<(), io::Error>;

pub fn pack(value: Value, out: &mut Write) -> PackResult {
    match value {
        Value::Null => pack_null(out),
        Value::Boolean(x) => pack_boolean(x, out),
        Value::Integer(x) => pack_integer(x, out),
        Value::Float(x) => pack_float(x, out),
        Value::String(x) => pack_string(&x[..], out),
        Value::List(items) => pack_list(items, out),
        Value::Map(items) => pack_map(items, out),
        Value::Structure { signature, fields } => pack_structure(signature, fields, out),
    }
}

fn pack_null(out: &mut Write) -> PackResult {
    out.write_u8(0xC0)
}

fn pack_boolean(value: bool, out: &mut Write) -> PackResult {
    out.write_u8(if value { 0xC3 } else { 0xC2 })
}

fn pack_integer(value: i64, out: &mut Write) -> PackResult {
    if -0x10 <= value && value < 0x80 {
        // TINY_INT
        out.write_i8(value as i8)
    } else if -0x80 <= value && value < 0x80 {
        // INT_8
        out.write_u8(0xC8)?;
        out.write_i8(value as i8)
    } else if -0x8000 <= value && value < 0x8000 {
        // INT_16
        out.write_u8(0xC9)?;
        out.write_i16::<BigEndian>(value as i16)
    } else if -0x8000_0000 <= value && value < 0x8000_0000 {
        // INT_32
        out.write_u8(0xCA)?;
        out.write_i32::<BigEndian>(value as i32)
    } else {
        // INT_64
        out.write_u8(0xCB)?;
        out.write_i64::<BigEndian>(value)
    }
}

fn pack_float(value: f64, out: &mut Write) -> PackResult {
    out.write_u8(0xC1)?;
    out.write_f64::<BigEndian>(value)
}

fn pack_string(value: &str, out: &mut Write) -> PackResult {
    let size: usize = value.len();
    if size < 0x10 {
        out.write_u8(0x80 + size as u8)?;
    } else if size < 0x100 {
        out.write_u8(0xD0)?;
        out.write_u8(size as u8)?;
    } else if size < 0x10000 {
        out.write_u8(0xD1)?;
        out.write_u16::<BigEndian>(size as u16)?;
    } else if size < 0x1_0000_0000 {
        out.write_u8(0xD2)?;
        out.write_u32::<BigEndian>(size as u32)?;
    } else {
        panic!("String too long to pack");
    }
    out.write_all(value.as_bytes())
}

fn pack_list(value: Vec<Value>, out: &mut Write) -> PackResult {
    let size = value.len();
    if size < 0x10 {
        out.write_u8(0x90 + size as u8)?;
    } else if size < 0x100 {
        out.write_u8(0xD4).unwrap();
        out.write_u8(size as u8)?;
    } else if size < 0x10000 {
        out.write_u8(0xD5).unwrap();
        out.write_u16::<BigEndian>(size as u16)?;
    } else if size < 0x1_0000_0000 {
        out.write_u8(0xD6).unwrap();
        out.write_u32::<BigEndian>(size as u32)?;
    } else {
        panic!("List too big to pack");
    }
    for val in value{
        val.pack(out)?;
    }
    Ok(())
}

fn pack_map(value: HashMap<String, Value>, out: &mut Write) -> PackResult {
    let size = value.len();
    if size < 0x10 {
        out.write_u8(0xA0 + size as u8)?;
    } else if size < 0x100 {
        out.write_u8(0xD8)?;
        out.write_u8(size as u8)?;
    } else if size < 0x10000 {
        out.write_u8(0xD9)?;
        out.write_u16::<BigEndian>(size as u16)?;
    } else if size < 0x1_0000_0000 {
        out.write_u8(0xDA)?;
        out.write_u32::<BigEndian>(size as u32)?;
    } else {
        panic!("Map too big to pack");
    }
    for (key, val) in value {
        pack_string(&key[..], out)?;
        val.pack(out)?;
    }
    Ok(())
}

fn pack_structure(signature: u8, fields: Vec<Value>, out: &mut Write) -> PackResult {
    let size = fields.len();
    if size < 0x10 {
        out.write_u8(0xB0 + size as u8)?;
    } else if size < 0x100 {
        out.write_u8(0xDC)?;
        out.write_u8(size as u8)?;
    } else if size < 0x10000 {
        out.write_u8(0xDD)?;
        out.write_u16::<BigEndian>(size as u16)?;
    } else {
        panic!("Structure too big to pack");
    }
    out.write_u8(signature)?;
    for val in fields {
        val.pack(out)?;
    }
    Ok(())
}
