use std::convert::TryInto;
use std::io::{Error, ErrorKind};

pub struct Partition {
    pub name: String,
    pub segments: Vec<String>,
    pub active_segment: String,
    pub rel_offset: u32,
}

impl Partition {
    pub fn new(name: &str) -> Self {
        Partition {
            name: name.to_string(),
            segments: Vec::new(),
            active_segment: String::new(),
            rel_offset: 0,
        }
    }
}

pub fn write_records(partition: &mut Partition, data: &[u8]) -> Result<(), std::io::Error> {
    // parse record bytes to structs
    let mut i = 0;
    let n = data.len();
    while i + 4 <= n {
        // read 4 bytes to get msg length
        let msg_len_bytes: [u8; 4] = data[i..i + 4].try_into().unwrap();
        let msg_len = u32::from_be_bytes(msg_len_bytes) as usize;

        i += 4;
        if i + msg_len > n {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "MESSAGE BYTES NOT FOUND",
            ));
        }

        // read X bytes to get msg (X = msg_len)
        let msg_bytes = &data[i..i + msg_len];
        let msg = std::str::from_utf8(msg_bytes)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "bad string of msg"))?;

        i += msg_len;
        if i + 4 + 8 > n {
            return Err(Error::new(ErrorKind::InvalidData, "CRC AND TS NOT FOUND"));
        }

        // read 4 bytes to get the crc
        let crc_bytes: [u8; 4] = data[i..i + 4].try_into().unwrap();
        let crc = u32::from_be_bytes(crc_bytes);
        // read 8 bytes to get the timestamp
        let ts_bytes: [u8; 8] = data[i + 4..i + 12].try_into().unwrap();
        let ts = u64::from_be_bytes(ts_bytes);

        i += 4 + 8;

        partition.rel_offset += 1;
        println!("[{}][{}][{}][{}]", msg_len, msg, crc, ts)
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_records_valid_data() {
        let mut partition = Partition::new("test-partition");

        // Construct a valid record:
        // 4 bytes: length (5)
        // 5 bytes: "hello"
        // 4 bytes: CRC (0xDEADBEEF)
        // 8 bytes: Timestamp (12345678)
        let mut data = Vec::new();
        data.extend_from_slice(&(5u32.to_be_bytes()));
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&(0xDEADBEEFu32.to_be_bytes()));
        data.extend_from_slice(&(12345678u64.to_be_bytes()));

        data.extend_from_slice(&(5u32.to_be_bytes()));
        data.extend_from_slice(b"world");
        data.extend_from_slice(&(0xDEADBFEFu32.to_be_bytes()));
        data.extend_from_slice(&(12545778u64.to_be_bytes()));

        data.extend_from_slice(&(8u32.to_be_bytes()));
        data.extend_from_slice(b"abhijeet");
        data.extend_from_slice(&(0xDEADBEEFu32.to_be_bytes()));
        data.extend_from_slice(&(12345678u64.to_be_bytes()));

        let result = write_records(&mut partition, &data);

        assert!(result.is_ok());
        assert_eq!(partition.rel_offset, 3);
    }

    #[test]
    fn test_write_records_invalid_utf8() {
        let mut partition = Partition::new("test-partition");

        let mut data = Vec::new();
        data.extend_from_slice(&(2u32.to_be_bytes()));
        data.extend_from_slice(&[0, 159]); // Invalid UTF-8
        data.extend_from_slice(&(0u32.to_be_bytes()));
        data.extend_from_slice(&(0u64.to_be_bytes()));

        let result = write_records(&mut partition, &data);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_write_records_invalid_crc() {
        let mut partition = Partition::new("test-partition");

        let mut data = Vec::new();
        data.extend_from_slice(&(2u32.to_be_bytes()));
        data.extend_from_slice(b"he");
        data.extend_from_slice(&(0u8.to_be_bytes()));
        data.extend_from_slice(&(0u64.to_be_bytes()));

        let result = write_records(&mut partition, &data);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_write_records_invalid_msg_len() {
        let mut partition = Partition::new("test-partition");

        let mut data = Vec::new();
        data.extend_from_slice(&(5u32.to_be_bytes()));
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&(0xDEADBEEFu32.to_be_bytes()));
        data.extend_from_slice(&(12345678u64.to_be_bytes()));

        data.extend_from_slice(&(5u32.to_be_bytes()));
        data.extend_from_slice(b"world");
        data.extend_from_slice(&(0xDEADBEEFu32.to_be_bytes()));
        data.extend_from_slice(&(12545778u64.to_be_bytes()));

        data.extend_from_slice(&(8u32.to_be_bytes()));
        data.extend_from_slice(b"abhijee");
        data.extend_from_slice(&(0xDEADBEEFu32.to_be_bytes()));
        data.extend_from_slice(&(12345678u64.to_be_bytes()));

        let result = write_records(&mut partition, &data);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
    }
}
