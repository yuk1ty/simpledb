use std::{io::Cursor, mem::size_of};

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};

pub struct Page {
    bb: BytesMut,
}

impl Page {
    pub fn with_capacity(block_size: usize) -> Self {
        Self {
            bb: BytesMut::with_capacity(block_size),
        }
    }

    pub fn new(bb: Vec<u8>) -> Self {
        Self {
            bb: BytesMut::from(bb.as_slice()),
        }
    }

    pub fn get_int(&self, offset: usize) -> i32 {
        let freezed = self.bb.clone().freeze();
        let mut rdr = Cursor::new(freezed.slice(offset..).to_vec());
        rdr.read_i32::<BigEndian>().unwrap()
    }

    pub fn set_int(&mut self, offset: usize, n: i32) {
        let n = n.to_be_bytes();

        unsafe {
            // if offset is set, advance the cursor by the offset in advance
            self.bb.advance_mut(offset);
        }

        for nn in n {
            unsafe {
                self.bb.chunk_mut().as_mut_ptr().write(nn);
                self.bb.advance_mut(1);
            }
        }
    }

    pub fn get_bytes(&mut self, offset: usize) -> Option<&[u8]> {
        let length = self.get_int(offset) as usize;
        // considering 4 byte header which is set while processing `set_bytes` function
        let baseline = offset + size_of::<i32>();
        let bytes = self.bb.get(baseline..(baseline + length));
        bytes
    }

    pub fn set_bytes(&mut self, offset: usize, b: &[u8]) {
        self.set_int(offset, b.len() as i32);
        self.bb.put(b);
    }

    pub fn get_string(&mut self, offset: usize) -> Option<String> {
        let b = self.get_bytes(offset);
        b.map(|bytes| unsafe { String::from_utf8_unchecked(bytes.to_vec()) })
    }

    pub fn set_string(&mut self, offset: usize, s: impl Into<String>) {
        let text = s.into();
        let b = text.as_bytes();
        self.set_bytes(offset, b);
    }

    pub(crate) fn contents(self) -> Bytes {
        // TODO might not correct
        self.bb.freeze()
    }
}

pub fn max_length(strlen: usize) -> usize {
    size_of::<u32>() + strlen * size_of::<char>()
}

#[cfg(test)]
mod test {
    use super::{max_length, Page};

    #[test]
    fn test_string_write() {
        let mut p1 = Page::with_capacity(400);
        let pos1: usize = 88;
        p1.set_string(pos1, "abcdefghijklm");
        assert_eq!(p1.get_string(pos1).unwrap(), "abcdefghijklm".to_string());
    }

    #[test]
    fn test_max_length() {
        let text = "abcdefghijklm".len();
        let max_length = max_length(text);
        assert_eq!(max_length, 56);
    }
}
