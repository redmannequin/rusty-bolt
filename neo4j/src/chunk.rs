use std::io::{self, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const MAX_CHUNK_SIZE: usize = 0xFFFF;

type ChunkResult<T> = Result<T, io::Error>;

pub struct ChunkStream<T: Read + Write> {
    stream: T,
}

impl<T: Read + Write> ChunkStream<T> {
    pub fn new(stream: T) -> Self {
        ChunkStream { stream }
    }

    pub fn send(&mut self, buf: &[u8]) -> ChunkResult<()> {
        for chunk in buf.chunks(MAX_CHUNK_SIZE) {
            self.stream.write_u16::<BigEndian>(chunk.len() as u16)?;
            self.stream.write_all(chunk)?;
        }
        self.stream.write_u16::<BigEndian>(0)?;
        Ok(())
    }

    pub fn recv(&mut self) -> ChunkResult<Vec<u8>> {
        let mut ret = Vec::new();
        let mut size = self.stream.read_u16::<BigEndian>()?;
        while size != 0 {
            (&mut self.stream)
                .take(u64::from(size))
                .read_to_end(&mut ret)?;
            size = self.stream.read_u16::<BigEndian>()?;
        }
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_read_test() {
        let buf: &mut [u8] = &mut [0, 5, 0, 1, 2, 3, 4, 0, 0];
        let mut c = ChunkStream::new(::std::io::Cursor::new(buf));
        let rbuf = c.recv().unwrap();
        for i in 0..5 {
            assert_eq!(i as u8, rbuf[i]);
        }
    }

    #[test]
    fn round_trip() {
        let mut c = ChunkStream::new(::std::io::Cursor::new(Vec::new()));
        for _ in 0..5 {
            c.send(&((0..100).into_iter().collect::<Vec<u8>>())[..])
                .unwrap();
        }
        c.stream.set_position(0);
        for _ in 0..5 {
            let rbuf = c.recv().unwrap();
            assert_eq!(rbuf.len(), 100);
            for i in 0..100 {
                assert_eq!(i as u8, rbuf[i]);
            }
        }
    }
}
