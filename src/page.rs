use bytes::{Buf, BufMut, Bytes, BytesMut};

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

    pub fn get_int(&mut self, offset: usize) -> i32 {
        self.bb.advance(offset);
        self.bb.get_i32()
    }

    pub fn set_int(&mut self, offset: usize, n: i32) {
        self.bb.advance(offset);
        self.bb.put_i32(n);
    }

    pub fn get_bytes(&mut self, offset: usize) -> Option<&[u8]> {
        self.bb.advance(offset);
        let length = self.bb.get_i32() as usize;
        // TODO ちょっと気になってる。あってる？
        self.bb.get(..length)
    }

    pub fn set_bytes(&mut self, offset: usize, b: &[u8]) {
        self.bb.advance(offset);
        self.bb.put_i32(b.len() as i32);
        self.bb.put(b);
    }

    pub fn get_string(&mut self, offset: usize) -> Option<String> {
        let b = self.get_bytes(offset);
        b.map(|bytes| unsafe { String::from_utf8_unchecked(bytes.to_vec()) })
    }

    pub(crate) fn contents(&mut self) -> Bytes {
        self.bb.advance(0);
        let bb = std::mem::take(&mut self.bb);
        bb.freeze()
    }
}

#[cfg(test)]
mod test {}
