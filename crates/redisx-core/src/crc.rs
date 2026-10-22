//! CRC16 (XMODEM, used for CLUSTER key-slot hashing) and CRC64 (used for RDB
//! checksums) — ported bit-for-bit from `redis_src/src/crc16.c` and
//! `redis_src/src/crc64.c` so redisX computes byte-identical values to the
//! original Redis for the same input. Both are verified below against the
//! exact self-test vectors embedded in the C sources.
//!
//! `crcspeed.c`'s 8-way sliced table trick (a performance optimization
//! producing numerically identical output to the bit-by-bit algorithm below)
//! is not ported in Phase 0; see `redis_document/25_redisX.md`.

/// CRC-16/XMODEM table, copied verbatim from `crc16.c`'s `crc16tab`.
const CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad,
    0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a,
    0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b,
    0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861,
    0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87,
    0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a,
    0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3,
    0x5004, 0x4025, 0x7046, 0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290,
    0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e,
    0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f,
    0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c,
    0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83,
    0x1ce0, 0x0cc1, 0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

/// crc16 — Width 16, Poly 0x1021, Init 0x0000, no reflection, no xorout.
/// `crc16("123456789") == 0x31c3`.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc = (crc << 8) ^ CRC16_TAB[(((crc >> 8) ^ b as u16) & 0x00ff) as usize];
    }
    crc
}

/// Width 64, Poly 0xad93d23594c935a9 (the Jones polynomial), RefIn=true,
/// RefOut=true, Init/XorOut as passed in by the caller (Redis always calls
/// with crc=0 and never XORs the output). Copied from `crc64.c`'s `POLY`.
const CRC64_POLY: u64 = 0xad93d235_94c935a9;

fn reflect64(mut data: u64) -> u64 {
    let mut out = 0u64;
    for _ in 0..64 {
        out = (out << 1) | (data & 1);
        data >>= 1;
    }
    out
}

/// `_crc64`'s bit-reversed polynomial. `crc64.c` itself derives the same
/// relationship for its benchmark combine function (`BENCH_RPOLY =
/// 0x95ac9329ac4bc9b5`, the reverse of `POLY`): operating in the
/// already-reflected domain throughout (instead of reflecting once at the
/// very end, as the `_crc64` reference/self-test function does) is what makes
/// the checksum resumable across chunks — required because Redis computes
/// RDB/AOF checksums incrementally as `cksum = crc64(cksum, buf, len)` across
/// many `rio` writes (see `rio.c`'s `rioGenericUpdateChecksum`).
fn crc64_rpoly() -> u64 {
    reflect64(CRC64_POLY)
}

/// crc64 — resumable reflected-CRC64, byte-identical to Redis's table-based
/// `crc64()` (which is itself numerically identical to `_crc64()`'s bit-by-bit
/// reference for a single complete buffer, but — unlike `_crc64` — safe to
/// call repeatedly across chunks of one logical stream). Call with `crc=0`
/// for a fresh checksum, or with a previous return value to continue hashing
/// more data: `crc64(crc64(0, a), b) == crc64(0, [a, b].concat())`.
///
/// `crc64(0, b"123456789") == 0xe9c6d914c4b8d9ca`.
pub fn crc64(crc: u64, data: &[u8]) -> u64 {
    let rpoly = crc64_rpoly();
    let mut crc = crc;
    for &byte in data {
        crc ^= byte as u64;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ rpoly } else { crc >> 1 };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_matches_c_self_test_vector() {
        // Comment in crc16.c: Output for "123456789" : 31C3
        assert_eq!(crc16(b"123456789"), 0x31c3);
    }

    #[test]
    fn crc64_matches_c_self_test_vectors() {
        // crc64.c's REDIS_TEST block:
        //   printf("[calcula]: e9c6d914c4b8d9ca == %016" PRIx64 "\n", _crc64(0, "123456789", 9));
        assert_eq!(crc64(0, b"123456789"), 0xe9c6d914c4b8d9ca);

        let lorem = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed \
do eiusmod tempor incididunt ut labore et dolore magna \
aliqua. Ut enim ad minim veniam, quis nostrud exercitation \
ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis \
aute irure dolor in reprehenderit in voluptate velit esse \
cillum dolore eu fugiat nulla pariatur. Excepteur sint \
occaecat cupidatat non proident, sunt in culpa qui officia \
deserunt mollit anim id est laborum.\0";
        assert_eq!(crc64(0, lorem), 0xc7794709e69683b3);
    }

    #[test]
    fn crc64_is_incremental() {
        let whole = crc64(0, b"123456789");
        let split = crc64(crc64(0, b"1234"), b"56789");
        assert_eq!(whole, split);
    }
}
