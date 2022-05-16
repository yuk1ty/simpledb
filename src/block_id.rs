use std::fmt::Display;

pub struct BlockId {
    pub filename: String,
    pub blknum: i32,
}

impl BlockId {
    pub fn new(filename: impl Into<String>, blknum: i32) -> Self {
        Self {
            filename: filename.into(),
            blknum,
        }
    }

    pub fn filename(&self) -> &str {
        self.filename.as_str()
    }

    pub fn number(&self) -> i32 {
        self.blknum
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}
