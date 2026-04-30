pub struct LogRecord {
    // len: u64,
    // rel_offset: u32,
    msg_len: u32,
    msg: Vec<u8>,
    crc: u32,
    timestamp: u64,
}
