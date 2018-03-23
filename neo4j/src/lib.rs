#[macro_use]
extern crate log;

#[macro_use]
extern crate packstream;

extern crate byteorder;

pub mod bolt;
pub mod cypher;
mod chunk;

use std::collections::HashMap;

use cypher::{CypherStream, StatementResult};
use bolt::{BoltError, BoltSummary};
pub use packstream::{Data, Value};

pub enum Neo4jError {
    ConnectFailure(BoltError),
    CommitFailure(HashMap<String, Value>),
    CommitNoSummary,
    RunFailure(HashMap<String, Value>),
    ClosedTransaction,
}

impl ::std::fmt::Debug for Neo4jError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Neo4jError::ConnectFailure(ref e) => writeln!(f, "Failure to connect: {:?}", e),
            Neo4jError::CommitFailure(ref e) => writeln!(f, "Failure to commit: {:?}", e),
            Neo4jError::CommitNoSummary => writeln!(f, "Commit returned no summary"),
            Neo4jError::RunFailure(ref e) => writeln!(f, "Failed to RUN: {:?}", e),
            Neo4jError::ClosedTransaction => writeln!(f, "Tried to operate on closed transaction"),
        }
    }
}

pub trait Neo4jOperations {
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> NeoResult<QueryResult>;
    fn run_unchecked(&mut self, statement: &str, parameters: HashMap<&str, Value>);
}

pub type NeoResult<T> = Result<T, Neo4jError>;

pub struct Neo4jTransaction<'a>(&'a mut Neo4jDB, bool);

impl<'a> Neo4jTransaction<'a> {
    fn new(neo: &'a mut Neo4jDB) -> Self {
        let mut s = Neo4jTransaction(neo, false);
        s._start();
        s
    }

    fn _start(&mut self) {
        self.0.conn.begin_transaction(None);
    }

    fn _commit(&mut self) -> NeoResult<HashMap<String, Value>> {
        match self.0.conn.commit_transaction() {
            Some(s) => match s {
                BoltSummary::Failure(m) => Err(Neo4jError::CommitFailure(m)),
                BoltSummary::Ignored(_) => unreachable!(),
                BoltSummary::Success(m) => Ok(m),
            },
            None => Err(Neo4jError::CommitNoSummary),
        }
    }

    fn _rollback(&mut self) {
        self.0.conn.rollback_transaction();
    }

    pub fn commit_and_refresh(&mut self) -> NeoResult<HashMap<String, Value>> {
        let ret = self._commit();
        if ret.is_ok() {
            self._start();
        }
        ret
    }

    pub fn commit(mut self) -> NeoResult<HashMap<String, Value>> {
        self.1 = true;
        self._commit()
    }

    pub fn rollback(mut self) {
        self.1 = true;
        self._rollback();
    }
}

impl<'a> Neo4jOperations for Neo4jTransaction<'a> {
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> NeoResult<QueryResult> {
        self.0.run(statement, parameters)
    }

    fn run_unchecked(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        self.0.run_unchecked(statement, parameters)
    }
}

impl<'a> Drop for Neo4jTransaction<'a> {
    fn drop(&mut self) {
        if !self.1 {
            self._rollback()
        }
    }
}

pub struct Neo4jDB {
    conn: CypherStream,
}

impl Neo4jDB {
    pub fn connect(addr: &str, user: &str, pass: &str) -> NeoResult<Self> {
        match CypherStream::connect(addr, user, pass) {
            Ok(s) => Ok(Neo4jDB { conn: s }),
            Err(e) => Err(Neo4jError::ConnectFailure(e)),
        }
    }

    pub fn transaction(&mut self) -> Neo4jTransaction {
        Neo4jTransaction::new(self)
    }
}

impl Neo4jOperations for Neo4jDB {
    fn run(&mut self, statement: &str, parameters: HashMap<&str, Value>) -> NeoResult<QueryResult> {
        let result = self.conn
            .run(statement, parameters)
            .map_err(Neo4jError::RunFailure)?;
        Ok(QueryResult::new(result, &mut self.conn))
    }

    fn run_unchecked(&mut self, statement: &str, parameters: HashMap<&str, Value>) {
        self.conn.run_unchecked(statement, parameters)
    }
}

pub struct QueryResult<'a> {
    src: StatementResult,
    conn: &'a mut CypherStream,
}

impl<'a> QueryResult<'a> {
    fn new(src: StatementResult, conn: &'a mut CypherStream) -> Self {
        QueryResult { src, conn }
    }

    pub fn first(self) -> Neo4jSingleIter<'a> {
        Neo4jSingleIter { inner: self }
    }

    pub fn maps(self) -> Neo4jMapIter<'a> {
        let keys = self.keys();
        Neo4jMapIter { inner: self, keys  }
    }

    pub fn keys(&self) -> Vec<String> {
        self.src
            .keys()
            .clone()
            .into_vec()
            .unwrap()
            .into_iter()
            .map(|v| v.into_string().unwrap())
            .collect()
    }
}

pub struct Neo4jSingleIter<'a> {
    inner: QueryResult<'a>,
}

pub struct Neo4jMapIter<'a> {
    inner: QueryResult<'a>,
    keys: Vec<String>,
}

impl<'a> Iterator for QueryResult<'a> {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        self.conn.fetch(&self.src)
    }
}

impl<'a> Drop for QueryResult<'a> {
    fn drop(&mut self) {
        while self.conn.fetch(&self.src).is_some() {}
        self.conn.fetch_summary(&self.src);
    }
}

impl<'a> Iterator for Neo4jSingleIter<'a> {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|Data::Record(mut v)| {
            assert_eq!(v.len(), 1);
            v.remove(0)
        })
    }
}

impl<'a> Iterator for Neo4jMapIter<'a> {
    type Item = HashMap<String, Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|Data::Record(v)| self.keys.clone().into_iter().zip(v).collect())
    }
}
