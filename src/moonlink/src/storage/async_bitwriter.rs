use more_asserts as ma;
use std::io;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_bitstream_io::{BitQueue, Endianness, Numeric};

/// Buffer size, which controls the flush threshold.
const BUFFER_SIZE: usize = 4096;

/// [`BitWriter`] implement asynchronous write for bits;
/// - Uses a cooperative interface: users are responsible for flushing when needed (check [`write`] return value).
/// - Buffers bits into memory to avoid frequent syscalls and runtime suspension from `await`.
///
/// Example usage:
/// let writer = AsyncBitWriter(internal_writer);
///
/// // If known in advance that overall bits to write is less than buffer size, callers could safely buffer them all before flush,
/// // allowing to avoid flushing mid-way and eliminate unnecessary `await` calls.
/// let _ = writer.write(some bits);
/// let _ = writer.write(some bits);
/// let to_flush = writer.write(some bits);
/// if to_flush {
///   writer.flush().await;
/// }
///
/// // Finalize any partial byte before flush.
/// let _ = writer.write(some bits);
///
/// // It's required to make sure alignment before final flush.
/// writer.byte_align();
/// writer.flush().await;
pub struct BitWriter<W: AsyncWrite + Unpin + Send + Sync, E: Endianness> {
    /// Async writer.
    writer: W,
    /// Two alternating buffers.
    buffers: [[u8; BUFFER_SIZE]; 2],
    /// Points to currently active buffer.
    active_buffer_index: usize,
    /// Builds 8-bit values from bits.
    /// All bits goes to [`bitqueue`] first then buffer when queue full.
    bitqueue: BitQueue<E, u8>,
    /// Current write offset for the active buffer.
    active_buffer_pos: usize,
    /// Current write offset for the inactive buffer.
    inactive_buffer_pos: usize,
}

impl<W: AsyncWrite + Unpin + Send + Sync, E: Endianness> BitWriter<W, E> {
    /// Number of bits to hold in the bitqueue.
    const BITQUEUE_SIZE: u32 = 8;

    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buffers: [[0u8; BUFFER_SIZE]; 2],
            active_buffer_index: 0,
            bitqueue: BitQueue::new(),
            active_buffer_pos: 0,
            inactive_buffer_pos: 0,
        }
    }

    pub fn endian(writer: W, _endian: E) -> Self {
        BitWriter::<W, E>::new(writer)
    }

    /// Flush buffered current content.
    pub async fn flush(&mut self) -> io::Result<()> {
        if self.active_buffer_pos > 0 {
            let flushed_data = &self.buffers[self.active_buffer_index][..self.active_buffer_pos];
            self.writer.write_all(flushed_data).await?;
        }

        // Switch for inactive / active buffers.
        self.active_buffer_pos = self.inactive_buffer_pos;
        self.inactive_buffer_pos = 0;
        self.active_buffer_index = 1 - self.active_buffer_index;

        self.writer.flush().await
    }

    /// Appends a byte to the active buffer or spills to the inactive buffer if full.
    /// Returns `true` if the active buffer was full and the byte was written to the inactive buffer.
    /// Panic if both buffers exceed their capacity, which should never happen in intended cooperative usage.
    fn buffer_byte(&mut self, byte: u8) -> bool {
        // There're free space at the active buffer.
        if self.active_buffer_pos < BUFFER_SIZE {
            let pos = self.active_buffer_pos;
            self.buffers[self.active_buffer_index][pos] = byte;
            self.active_buffer_pos += 1;
            return false;
        }

        // Write bytes to the inactive buffer, which will be switch to the active one later.
        self.buffers[1 - self.active_buffer_index][self.inactive_buffer_pos] = byte;
        self.inactive_buffer_pos += 1;
        true
    }

    /// Append one single bit, and return whether caller needs to flush.
    /// It's of the same effect as [`write`] with one single bit.
    #[must_use]
    pub fn write_bit(&mut self, bit: bool) -> bool {
        assert!(!self.bitqueue.is_full());
        self.bitqueue.push(1, bit as u8);
        if self.bitqueue.is_full() {
            let byte = self.bitqueue.pop(Self::BITQUEUE_SIZE);
            if self.buffer_byte(byte) {
                return true;
            }
        }
        false
    }

    /// Write [`value`] into buffer, and return whether caller needs to flush.
    /// Notice, this function never performs flushing itself — the caller must check the return value and call [`flush`] if needed.
    ///
    /// Internally, async bit writer leverages two buffers, with each of size 4KiB.
    /// So even if one fills up, the second backup buffer could still take new bits.
    /// It's used for performance optimization to reduce async functions and write calls.
    /// But if both buffer goes out of memory, it will panic.
    ///
    /// An example usage is
    /// func some_write_func() -> bool {
    ///   // It's safe to ignore the first two writes return value.
    ///   let _ = bitwriter.write(some bits);
    ///   let _ = bitwriter.write(some bits);
    ///   bitwriter.write(some bits)
    /// }
    /// If the overall bits to write is less than buffer size 4KiB, we don't need to declare [`some_write_func`] as `async` and flush immediately.
    #[must_use]
    pub fn write<U>(&mut self, bits: u32, value: U) -> bool
    where
        U: Numeric,
    {
        assert!(!self.bitqueue.is_full());
        ma::assert_le!(bits, U::BITS_SIZE);
        if bits < U::BITS_SIZE && value >= (U::ONE << bits) {
            panic!("value too large for number of bits");
        }

        let mut acc = BitQueue::<E, U>::from_value(value, bits);
        let mut to_flush = false;

        // First try to fill bitqueue, and flush to buffer if possible.
        let bits_to_fill = Self::BITQUEUE_SIZE - self.bitqueue.len();
        let n = bits_to_fill.min(acc.len());
        let bits_chunk = acc.pop(n).to_u8();
        self.bitqueue.push(n, bits_chunk);

        // Bitqueue hasn't been full, no need to flush to buffer.
        if !self.bitqueue.is_full() {
            return false;
        }

        // Bitqueue is already full, transfer to buffer.
        let byte = self.bitqueue.pop(Self::BITQUEUE_SIZE);
        to_flush |= self.buffer_byte(byte);

        // Now try to move bits in "bytes granularity" directly to buffer, without going through bitqueue.
        while acc.len() >= Self::BITQUEUE_SIZE {
            let byte = acc.pop(Self::BITQUEUE_SIZE).to_u8();
            to_flush |= self.buffer_byte(byte);
        }

        // Enqueue left bits to bitqueue.
        if !acc.is_empty() {
            let n = acc.len();
            let bits_chunk = acc.pop(n).to_u8();
            self.bitqueue.push(n, bits_chunk);
        }

        to_flush
    }

    /// Pads the current bit queue with zeros until aligned to the next byte boundary.
    /// This should be called before the final flush to ensure the output is byte-aligned.
    /// Returns `true` if a flush is required due to buffer spill.
    pub fn byte_align(&mut self) -> bool {
        let cur_bit_len = self.bitqueue.len();
        if cur_bit_len == 0 {
            return false;
        }
        let mut to_flush = false;
        for _ in cur_bit_len..8 {
            to_flush = self.write_bit(false);
        }
        to_flush
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufWriter;
    use tokio_bitstream_io::BigEndian;

    #[tokio::test]
    async fn test_bitwriter_flush_empty() {
        let inner = Vec::new();
        let writer = BufWriter::new(inner);
        let mut bit_writer = BitWriter::<_, BigEndian>::new(writer);
        bit_writer.flush().await.unwrap();

        let result = bit_writer.writer.into_inner();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_bitwriter_buffered_write_and_flush() {
        let inner = Vec::new();
        let writer = BufWriter::new(inner);
        let mut bit_writer = BitWriter::<_, BigEndian>::new(writer);

        assert!(!bit_writer.write_bit(true));
        assert!(!bit_writer.write_bit(false));
        assert!(!bit_writer.write_bit(true));
        assert!(!bit_writer.write_bit(true));
        assert!(!bit_writer.write_bit(false));
        assert!(!bit_writer.write_bit(false));
        assert!(!bit_writer.write_bit(false));
        assert!(!bit_writer.write_bit(true)); // 0b10110001 = 0xB1

        assert!(!bit_writer.write::<u8>(4, 0b1111)); // write 4 bits: 1111
        bit_writer.byte_align(); // pad 4 bits: 0xF0

        bit_writer.flush().await.unwrap();
        let result = bit_writer.writer.into_inner();
        assert_eq!(result, vec![0xB1, 0xF0]);
    }

    /// Testing scenario: write 1 bit every time, and overall writes require buffer switching.
    #[tokio::test]
    async fn test_bitwriter_1bit_flush_across_buffers() {
        let inner = Vec::new();
        let writer = BufWriter::new(inner);
        let mut bit_writer = BitWriter::<_, BigEndian>::new(writer);

        let mut switched = false;
        let mut bit_index: u8 = 0;

        for _ in 0..(5000 * 8) {
            let to_flush = bit_writer.write_bit(true);
            if to_flush {
                switched = true;
                bit_writer.flush().await.unwrap();
            }
            bit_index += 1;
            if bit_index == 8 {
                bit_index = 0;
            }
        }

        bit_writer.byte_align();
        bit_writer.flush().await.unwrap();

        let result = bit_writer.writer.into_inner();
        assert_eq!(result.len(), 5000);
        for (i, byte) in result.iter().enumerate() {
            assert_eq!(*byte, 0b1111_1111, "mismatch at index {}", i);
        }
        assert!(switched);
    }

    /// Testing scenario: write aligned 8 bits everytime, and overall writes requires buffer switching.
    #[tokio::test]
    async fn test_bitwriter_aligned_8bit_flush_across_buffers() {
        let inner = Vec::new();
        let writer = BufWriter::new(inner);
        let mut bit_writer = BitWriter::<_, BigEndian>::new(writer);

        let mut switched = false;
        for i in 0..5000 {
            let to_flush = bit_writer.write::<u8>(8, (i % 256) as u8);
            if to_flush {
                switched = true;
                bit_writer.flush().await.unwrap();
            }
        }

        bit_writer.byte_align();
        bit_writer.flush().await.unwrap();

        let result = bit_writer.writer.into_inner();
        assert_eq!(result.len(), 5000);
        for (i, byte) in result.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "mismatch at index {}", i);
        }
        assert!(switched);
    }

    /// Testing scenario: write unaligned 7 bits everytime, and overall writes requires buffer switching.
    #[tokio::test]
    async fn test_bitwriter_unaligned_7bit_flush_across_buffers() {
        let inner = Vec::new();
        let writer = BufWriter::new(inner);
        let mut bit_writer = BitWriter::<_, BigEndian>::new(writer);

        let mut switched = false;
        for i in 0..5000 {
            let to_flush = bit_writer.write::<u8>(7, (i % 128) as u8);
            if to_flush {
                switched = true;
                bit_writer.flush().await.unwrap();
            }
        }

        bit_writer.byte_align();
        bit_writer.flush().await.unwrap();

        let result = bit_writer.writer.into_inner();
        let mut bits = Vec::with_capacity(result.len() * 8);
        for byte in &result {
            for i in (0..8).rev() {
                bits.push((byte >> i) & 1);
            }
        }

        let mut decoded = Vec::new();
        let mut idx = 0;

        for _ in 0..5000 {
            let mut val = 0u8;
            for _ in 0..7 {
                val <<= 1;
                val |= bits[idx];
                idx += 1;
            }
            decoded.push(val);
        }
        for (i, actual) in decoded.iter().enumerate() {
            assert_eq!(*actual, (i % 128) as u8, "mismatch at index {}", i);
        }
        assert!(switched);
    }

    /// Testing scenario: value to append is larger than bitqueue capacity.
    #[tokio::test]
    async fn test_bitwriter_write_u16_cross_bitqueue_boundary() {
        let inner = Vec::new();
        let writer = BufWriter::new(inner);
        let mut bit_writer = BitWriter::<_, BigEndian>::new(writer);

        // Write a full u16 = 0b10101010_11001100 = 0xAA, 0xCC
        let flush_required = bit_writer.write::<u16>(16, 0b1010_1010_1100_1100);
        assert!(!flush_required);

        bit_writer.byte_align();
        bit_writer.flush().await.unwrap();

        let result = bit_writer.writer.into_inner();
        assert_eq!(result, vec![0xAA, 0xCC]);
    }
}
