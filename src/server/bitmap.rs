use croaring::bitmap::Bitmap;
use std::{convert::TryInto, io::{Write, Read, Error}};

pub struct BitmapWrapper {
    bitmap: Bitmap
}

impl BitmapWrapper {
    pub fn new() -> Self {
        BitmapWrapper { bitmap: Bitmap::create() }
    }
}

impl Write for BitmapWrapper {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let casted = buf.iter().map(|x| *x as u32).collect::<Vec<u32>>().as_slice();
        self.bitmap.add_many(&casted);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
