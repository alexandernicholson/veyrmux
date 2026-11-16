//! Yamux v0 header codec. Payloads are opaque; no scanning or extra framing.
use bytes::BytesMut;
use std::io;

pub const HEADER_LEN: usize = 12;
pub const MAX_FRAME: usize = 1024 * 1024;
pub const SYN: u16 = 1;
pub const ACK: u16 = 2;
pub const FIN: u16 = 4;
pub const RST: u16 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Data = 0,
    WindowUpdate = 1,
    Ping = 2,
    GoAway = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub kind: FrameType,
    pub flags: u16,
    pub stream_id: u32,
    pub value: u32,
}

impl Header {
    #[inline]
    pub(crate) fn encode(self) -> [u8; HEADER_LEN] {
        let mut wire = [0; HEADER_LEN];
        wire[1] = self.kind as u8;
        wire[2..4].copy_from_slice(&self.flags.to_be_bytes());
        wire[4..8].copy_from_slice(&self.stream_id.to_be_bytes());
        wire[8..12].copy_from_slice(&self.value.to_be_bytes());
        wire
    }

    #[inline]
    pub(crate) fn append_to(self, wire: &mut BytesMut) {
        wire.extend_from_slice(&self.encode());
    }

    pub(crate) fn decode(wire: &[u8; HEADER_LEN]) -> io::Result<Self> {
        let invalid = |reason| io::Error::new(io::ErrorKind::InvalidData, reason);
        if wire[0] != 0 {
            return Err(invalid("unsupported Yamux version"));
        }
        let kind = match wire[1] {
            0 => FrameType::Data,
            1 => FrameType::WindowUpdate,
            2 => FrameType::Ping,
            3 => FrameType::GoAway,
            _ => return Err(invalid("unknown Yamux frame type")),
        };
        let flags = u16::from_be_bytes([wire[2], wire[3]]);
        if flags & !(SYN | ACK | FIN | RST) != 0 {
            return Err(invalid("unknown Yamux flags"));
        }
        let stream_id = u32::from_be_bytes([wire[4], wire[5], wire[6], wire[7]]);
        let value = u32::from_be_bytes([wire[8], wire[9], wire[10], wire[11]]);
        match kind {
            FrameType::Data | FrameType::WindowUpdate if stream_id == 0 => {
                return Err(invalid("stream frame uses reserved stream ID zero"));
            }
            FrameType::Ping | FrameType::GoAway if stream_id != 0 => {
                return Err(invalid("session frame uses a nonzero stream ID"));
            }
            FrameType::Data if value as usize > MAX_FRAME => {
                return Err(invalid("Yamux data frame exceeds receive limit"));
            }
            FrameType::Ping if flags != SYN && flags != ACK => {
                return Err(invalid("ping must be a request or an acknowledgement"));
            }
            FrameType::GoAway if flags != 0 || value > 2 => {
                return Err(invalid("invalid GoAway flags or error code"));
            }
            _ => {}
        }
        Ok(Self {
            kind,
            flags,
            stream_id,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn data_header_matches_yamux_big_endian_wire_format() {
        let header = Header {
            kind: FrameType::Data,
            flags: SYN | FIN,
            stream_id: 0x0102_0305,
            value: 0x0001_0203,
        };
        let wire = [0, 0, 0, 5, 1, 2, 3, 5, 0, 1, 2, 3];
        assert_eq!(header.encode(), wire);
        assert_eq!(Header::decode(&wire).unwrap(), header);
        let mut buffer = BytesMut::new();
        header.append_to(&mut buffer);
        assert_eq!(buffer.as_ref(), wire);
    }

    #[test]
    fn session_frames_round_trip_and_bad_scope_is_rejected() {
        for header in [
            Header {
                kind: FrameType::WindowUpdate,
                flags: ACK,
                stream_id: 2,
                value: 262144,
            },
            Header {
                kind: FrameType::Ping,
                flags: SYN,
                stream_id: 0,
                value: 17,
            },
            Header {
                kind: FrameType::Ping,
                flags: ACK,
                stream_id: 0,
                value: 17,
            },
            Header {
                kind: FrameType::GoAway,
                flags: 0,
                stream_id: 0,
                value: 0,
            },
        ] {
            assert_eq!(Header::decode(&header.encode()).unwrap(), header);
        }
        for wire in [
            [1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
            [0, 4, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
            [0, 0, 0, 16, 0, 0, 0, 1, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 1, 0, 16, 0, 1],
            [0, 2, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0],
            [0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
            [0, 3, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
        ] {
            assert!(Header::decode(&wire).is_err(), "accepted {wire:?}");
        }
    }

    proptest! {
        #[test]
        fn accepted_headers_reencode_exactly(wire in any::<[u8; HEADER_LEN]>()) {
            if let Ok(header) = Header::decode(&wire) {
                prop_assert_eq!(header.encode(), wire);
            }
        }
    }
}
