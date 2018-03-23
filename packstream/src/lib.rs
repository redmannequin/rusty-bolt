#![feature(try_from)]

extern crate byteorder;

use std::fmt;
use std::hash::Hash;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::io::{Read, Write};
use std::convert::{TryFrom, TryInto};

mod pack;
mod unpack;

#[derive(Clone, PartialEq)]
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

impl Value {
    pub fn pack(self, out: &mut Write) -> pack::PackResult {
        pack::pack(self, out)
    }

    pub fn pack_into(self) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::new();
        self.pack(&mut buf)?;
        Ok(buf)
    }

    pub fn unpack(stream: &mut Read) -> unpack::UnpackResult {
        unpack::unpack(stream)
    }

    pub fn into_bool(self) -> Option<bool> {
        match self {
            Value::Boolean(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_int<T: TryFrom<i64>>(self) -> Option<T> {
        match self {
            Value::Integer(v) => v.try_into().ok(),
            _ => None,
        }
    }

    pub fn into_float<T: TryFrom<f64>>(self) -> Option<T> {
        match self {
            Value::Float(v) => v.try_into().ok(),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_vec(self) -> Option<Vec<Value>> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_map(self) -> Option<HashMap<String, Value>> {
        match self {
            Value::Map(v) => Some(v),
            _ => None,
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Null => write!(f, "null"),
            Value::Boolean(ref value) => write!(f, "{:?}", value),
            Value::Integer(ref value) => write!(f, "{:?}", value),
            Value::Float(ref value) => write!(f, "{:?}", value),
            Value::String(ref value) => write!(f, "{:?}", value),
            Value::List(ref values) => write!(f, "{:?}", values),
            Value::Map(ref values) => write!(f, "{:?}", values),
            Value::Structure {
                signature,
                ref fields,
            } => write!(f, "#{:02X} {:?}", signature, fields),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::List(ref values) => write_tsv(f, values),
            _ => write!(f, "{:?}", self),
        }
    }
}

pub fn write_tsv(f: &mut fmt::Formatter, values: &[Value]) -> fmt::Result {
    let last = values.len() - 1;
    for value in values[..last].iter() {
        let _ = write!(f, "{:?}\t", value);
    }
    write!(f, "{:?}", values[last])
}

macro_rules! impl_From_Integer {
    ($T:ty) => {
        impl From<$T> for Value {
            fn from(val: $T) -> Self {
                Value::Integer(val as i64)
            }
        }
    }
}

macro_rules! impl_From_Float {
    ($T:ty) => {
        impl From<$T> for Value {
            fn from(val: $T) -> Self {
               Value::Float(val as f64)
            }
        }
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self {
        Value::Boolean(val)
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(val: Vec<T>) -> Self {
        Value::List(val.into_iter().map(|e| e.into()).collect())
    }
}

impl<T> FromIterator<T> for Value
where
    T: Into<Value>,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Value::List(iter.into_iter().map(|v| v.into()).collect())
    }
}

impl<S, T> From<HashMap<S, T>> for Value
where
    S: ToString + Eq + Hash,
    T: Into<Value>,
{
    fn from(val: HashMap<S, T>) -> Self {
        Value::Map(
            val.into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect(),
        )
    }
}

impl<S, T> FromIterator<(S, T)> for Value
where
    S: ToString,
    T: Into<Value>,
{
    fn from_iter<I: IntoIterator<Item = (S, T)>>(iter: I) -> Self {
        Value::Map(
            iter.into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect(),
        )
    }
}

impl From<String> for Value {
    fn from(val: String) -> Self {
        Value::String(val)
    }
}

impl<'t> From<&'t str> for Value {
    fn from(val: &'t str) -> Self {
        Value::String(String::from(val))
    }
}

impl_From_Integer!(i8);
impl_From_Integer!(i16);
impl_From_Integer!(i32);
impl_From_Integer!(i64);
impl_From_Integer!(isize);

impl_From_Integer!(u8);
impl_From_Integer!(u16);
impl_From_Integer!(u32);
impl_From_Integer!(u64);
impl_From_Integer!(usize);

impl_From_Float!(f32);
impl_From_Float!(f64);

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

macro_rules! pat_cond {
    ($P:pat = $E:expr) => {
        match $E {
            $P => true,
            _ => false,
        }
    }
}

impl ValueMatch for Value {
    fn is_null(&self) -> bool {
        pat_cond!(Value::Null = *self)
    }

    fn is_boolean(&self) -> bool {
        pat_cond!(Value::Boolean(_) = *self)
    }

    fn is_integer(&self) -> bool {
        pat_cond!(Value::Integer(_) = *self)
    }

    fn is_float(&self) -> bool {
        pat_cond!(Value::Float(_) = *self)
    }

    fn is_string(&self) -> bool {
        pat_cond!(Value::String(_) = *self)
    }

    fn is_list(&self) -> bool {
        pat_cond!(Value::List(_) = *self)
    }

    fn is_map(&self) -> bool {
        pat_cond!(Value::Map(_) = *self)
    }

    fn is_structure(&self) -> bool {
        pat_cond!(Value::Structure{ .. } = *self)
    }
}

pub enum Data {
    Record(Vec<Value>),
}
impl fmt::Debug for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Data::Record(ref values) => write!(f, "Record({:?})", values),
        }
    }
}
impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Data::Record(ref data) => write_tsv(f, data),
        }
    }
}

#[macro_export]
macro_rules! parameters(
    {} => {
        {
            use std::collections::HashMap;

            HashMap::new()
        }
    };

    { $($key:expr => $value:expr),* } => {
        {
            use std::collections::HashMap;
            use $crate::Value;

            let mut map : HashMap<&str, Value> = HashMap::new();
            $(
                map.insert($key, $value.into());
            )+;

            map
        }
    };
);
