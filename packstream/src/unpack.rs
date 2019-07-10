use std::{
    collections::HashMap, io::{self, Read},
};

use byteorder::{BigEndian, ReadBytesExt};

use super::Value;

pub type UnpackResult = Result<Value, io::Error>;

pub fn unpack(stream: &mut dyn Read) -> UnpackResult {
    let marker = stream.read_u8()?;
    match marker {
        0x00..=0x7F => Ok(Value::Integer(i64::from(marker))),
        0x80..=0x8F => unpack_string((marker & 0x0F) as usize, stream),
        0x90..=0x9F => unpack_list((marker & 0x0F) as usize, stream),
        0xA0..=0xAF => unpack_map((marker & 0x0F) as usize, stream),
        0xB0..=0xBF => unpack_structure((marker & 0x0F) as usize, stream),
        0xC0 => Ok(Value::Null),
        0xC1 => Ok(Value::Float(stream.read_f64::<BigEndian>()?)),
        0xC2 => Ok(Value::Boolean(false)),
        0xC3 => Ok(Value::Boolean(true)),
        0xC8 => Ok(Value::Integer(i64::from(stream.read_i8()?))),
        0xC9 => Ok(Value::Integer(i64::from(stream.read_i16::<BigEndian>()?))),
        0xCA => Ok(Value::Integer(i64::from(stream.read_i32::<BigEndian>()?))),
        0xCB => Ok(Value::Integer(stream.read_i64::<BigEndian>()?)),
        0xD0 => {
            let size = stream.read_u8()? as usize;
            unpack_string(size, stream)
        }
        0xD1 => {
            let size = stream.read_u16::<BigEndian>()? as usize;
            unpack_string(size, stream)
        }
        0xD2 => {
            let size = stream.read_u32::<BigEndian>()? as usize;
            unpack_string(size, stream)
        }
        0xD4 => {
            let size = stream.read_u8()? as usize;
            unpack_list(size, stream)
        }
        0xD5 => {
            let size = stream.read_u16::<BigEndian>()? as usize;
            unpack_list(size, stream)
        }
        0xD6 => {
            let size = stream.read_u32::<BigEndian>()? as usize;
            unpack_list(size, stream)
        }
        0xD8 => {
            let size = stream.read_u8()? as usize;
            unpack_map(size, stream)
        }
        0xD9 => {
            let size = stream.read_u16::<BigEndian>()? as usize;
            unpack_map(size, stream)
        }
        0xDA => {
            let size = stream.read_u32::<BigEndian>()? as usize;
            unpack_map(size, stream)
        }
        0xDC => {
            let size = stream.read_u8()? as usize;
            unpack_structure(size, stream)
        }
        0xDD => {
            let size = stream.read_u16::<BigEndian>()? as usize;
            unpack_structure(size, stream)
        }
        0xF0..=0xFF => Ok(Value::Integer(i64::from(marker) - 0x100)),
        _ => panic!("Illegal value with marker {:02X}", marker),
    }
}

fn unpack_string(size: usize, stream: &mut dyn Read) -> UnpackResult {
    let mut s = String::with_capacity(size);
    stream.take(size as u64).read_to_string(&mut s)?;
    Ok(Value::String(s))
}

fn unpack_list(size: usize, stream: &mut dyn Read) -> UnpackResult {
    let mut value = Vec::with_capacity(size);
    for _ in 0..size {
        value.push(unpack(stream)?);
    }
    Ok(Value::List(value))
}

fn unpack_map(size: usize, stream: &mut dyn Read) -> UnpackResult {
    let mut value = HashMap::with_capacity(size);
    for _ in 0..size {
        let key = unpack(stream)?;
        match key {
            Value::String(k) => {
                value.insert(k, unpack(stream)?);
            }
            _ => panic!("Key is not a string"),
        }
    }
    Ok(Value::Map(value))
}

fn unpack_structure(size: usize, stream: &mut dyn Read) -> UnpackResult {
    let signature: u8 = stream.read_u8()?;
    let mut fields: Vec<Value> = Vec::with_capacity(size);
    for _ in 0..size {
        fields.push(unpack(stream)?);
    }
    Ok(Value::Structure { signature, fields })
}
