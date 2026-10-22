//! Sorted integer set, byte-compatible port of `redis_src/src/intset.c`.
//!
//! The on-disk/in-memory layout is always little-endian in the original C
//! (the `memrev*ifbe` macros only reverse bytes on big-endian hosts), so
//! `to_bytes`/`from_bytes` here reproduce the exact `encoding(u32 LE) ++
//! length(u32 LE) ++ elements(LE, sorted ascending)` blob Redis uses in RDB
//! files and in the `OBJ_ENCODING_INTSET` representation — this is required
//! for Phase 4 (RDB) to interoperate with real Redis dumps.

const ENC_INT16: u32 = 2;
const ENC_INT32: u32 = 4;
const ENC_INT64: u32 = 8;

fn value_encoding(v: i64) -> u32 {
    if v < i32::MIN as i64 || v > i32::MAX as i64 {
        ENC_INT64
    } else if v < i16::MIN as i64 || v > i16::MAX as i64 {
        ENC_INT32
    } else {
        ENC_INT16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntSet {
    encoding: u32,
    /// Raw little-endian element bytes; length is always `encoding * len()`.
    contents: Vec<u8>,
}

impl Default for IntSet {
    fn default() -> Self {
        Self::new()
    }
}

impl IntSet {
    /// intsetNew
    pub fn new() -> Self {
        IntSet { encoding: ENC_INT16, contents: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.contents.len() / self.encoding as usize
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    fn get_at(&self, pos: usize) -> i64 {
        let off = pos * self.encoding as usize;
        match self.encoding {
            ENC_INT16 => i16::from_le_bytes(self.contents[off..off + 2].try_into().unwrap()) as i64,
            ENC_INT32 => i32::from_le_bytes(self.contents[off..off + 4].try_into().unwrap()) as i64,
            ENC_INT64 => i64::from_le_bytes(self.contents[off..off + 8].try_into().unwrap()),
            _ => unreachable!("intset encoding is always 2, 4 or 8"),
        }
    }

    fn set_at(&mut self, pos: usize, value: i64) {
        let off = pos * self.encoding as usize;
        match self.encoding {
            ENC_INT16 => self.contents[off..off + 2].copy_from_slice(&(value as i16).to_le_bytes()),
            ENC_INT32 => self.contents[off..off + 4].copy_from_slice(&(value as i32).to_le_bytes()),
            ENC_INT64 => self.contents[off..off + 8].copy_from_slice(&value.to_le_bytes()),
            _ => unreachable!("intset encoding is always 2, 4 or 8"),
        }
    }

    /// intsetSearch — Ok(pos) if found, Err(insertion point) otherwise.
    fn search(&self, value: i64) -> Result<usize, usize> {
        let len = self.len();
        if len == 0 {
            return Err(0);
        }
        if value > self.get_at(len - 1) {
            return Err(len);
        }
        if value < self.get_at(0) {
            return Err(0);
        }
        let (mut lo, mut hi) = (0isize, len as isize - 1);
        while lo <= hi {
            let mid = ((lo + hi) as usize) >> 1;
            let cur = self.get_at(mid);
            match value.cmp(&cur) {
                std::cmp::Ordering::Greater => lo = mid as isize + 1,
                std::cmp::Ordering::Less => hi = mid as isize - 1,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo as usize)
    }

    /// intsetUpgradeAndAdd
    fn upgrade_and_add(&mut self, value: i64) {
        let newenc = value_encoding(value);
        let length = self.len();
        let prepend = value < 0;
        let old: Vec<i64> = (0..length).map(|i| self.get_at(i)).collect();

        self.encoding = newenc;
        self.contents = vec![0u8; (length + 1) * newenc as usize];
        let offset = if prepend { 1 } else { 0 };
        for (i, v) in old.into_iter().enumerate() {
            self.set_at(i + offset, v);
        }
        if prepend {
            self.set_at(0, value);
        } else {
            self.set_at(length, value);
        }
    }

    /// intsetAdd — returns `true` if the value was inserted, `false` if it was
    /// already present (set is unchanged).
    pub fn add(&mut self, value: i64) -> bool {
        let valenc = value_encoding(value);
        if valenc > self.encoding {
            self.upgrade_and_add(value);
            return true;
        }
        match self.search(value) {
            Ok(_) => false,
            Err(pos) => {
                let elem = self.encoding as usize;
                let at = pos * elem;
                self.contents.splice(at..at, std::iter::repeat_n(0u8, elem));
                self.set_at(pos, value);
                true
            }
        }
    }

    /// intsetRemove — returns `true` if the value was present and removed.
    pub fn remove(&mut self, value: i64) -> bool {
        let valenc = value_encoding(value);
        if valenc > self.encoding {
            return false;
        }
        match self.search(value) {
            Ok(pos) => {
                let elem = self.encoding as usize;
                self.contents.drain(pos * elem..(pos + 1) * elem);
                true
            }
            Err(_) => false,
        }
    }

    /// Same purpose as `ListPack::shrink_to_fit` — `remove`'s `drain` leaves
    /// `contents`'s allocated capacity at its historical peak.
    pub fn shrink_to_fit(&mut self) {
        self.contents.shrink_to_fit();
    }

    /// intsetFind
    pub fn find(&self, value: i64) -> bool {
        value_encoding(value) <= self.encoding && self.search(value).is_ok()
    }

    /// intsetMax
    pub fn max(&self) -> Option<i64> {
        if self.is_empty() {
            None
        } else {
            Some(self.get_at(self.len() - 1))
        }
    }

    /// intsetMin
    pub fn min(&self) -> Option<i64> {
        if self.is_empty() {
            None
        } else {
            Some(self.get_at(0))
        }
    }

    /// intsetGet
    pub fn get(&self, pos: usize) -> Option<i64> {
        if pos < self.len() {
            Some(self.get_at(pos))
        } else {
            None
        }
    }

    pub fn to_vec(&self) -> Vec<i64> {
        (0..self.len()).map(|i| self.get_at(i)).collect()
    }

    /// intsetBlobLen worth of bytes: `encoding(u32 LE) ++ length(u32 LE) ++ contents`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.contents.len());
        out.extend_from_slice(&self.encoding.to_le_bytes());
        out.extend_from_slice(&(self.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.contents);
        out
    }

    /// Inverse of `to_bytes`; also serves as `intsetValidateIntegrity(deep=0)`
    /// (header-only validation). Returns `None` on a malformed blob.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 8 {
            return None;
        }
        let encoding = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if ![ENC_INT16, ENC_INT32, ENC_INT64].contains(&encoding) {
            return None;
        }
        let length = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
        let needed = 8usize.checked_add(length.checked_mul(encoding as usize)?)?;
        if buf.len() != needed {
            return None;
        }
        Some(IntSet { encoding, contents: buf[8..].to_vec() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_adding_matches_c_test() {
        let mut is = IntSet::new();
        assert!(is.add(5));
        assert!(is.add(6));
        assert!(is.add(4));
        assert!(!is.add(4)); // duplicate
        assert_eq!(is.max(), Some(6));
        assert_eq!(is.min(), Some(4));
        assert_eq!(is.to_vec(), vec![4, 5, 6]);
    }

    #[test]
    fn shrink_to_fit_recovers_capacity_left_by_removal() {
        let mut is = IntSet::new();
        for i in 0..5000i64 {
            is.add(i);
        }
        let grown_capacity = is.contents.capacity();
        for i in 0..4999i64 {
            assert!(is.remove(i));
        }
        assert_eq!(is.to_vec(), vec![4999]);
        assert_eq!(is.contents.capacity(), grown_capacity, "drain must not have shrunk capacity on its own");
        is.shrink_to_fit();
        assert!(is.contents.capacity() < grown_capacity, "shrink_to_fit must actually free the unused capacity");
        assert_eq!(is.to_vec(), vec![4999], "shrink must not change the data");
    }

    #[test]
    fn upgrade_int16_to_int32_append() {
        let mut is = IntSet::new();
        is.add(32);
        assert_eq!(is.encoding, ENC_INT16);
        is.add(65535);
        assert_eq!(is.encoding, ENC_INT32);
        assert!(is.find(32));
        assert!(is.find(65535));
        assert_eq!(is.to_vec(), vec![32, 65535]);
    }

    #[test]
    fn upgrade_int16_to_int32_prepend_negative() {
        let mut is = IntSet::new();
        is.add(32);
        is.add(-65535);
        assert_eq!(is.encoding, ENC_INT32);
        assert!(is.find(32));
        assert!(is.find(-65535));
        assert_eq!(is.to_vec(), vec![-65535, 32]);
    }

    #[test]
    fn upgrade_int16_to_int64() {
        let mut is = IntSet::new();
        is.add(32);
        is.add(4294967295);
        assert_eq!(is.encoding, ENC_INT64);
        assert!(is.find(32));
        assert!(is.find(4294967295));
    }

    #[test]
    fn remove_and_find() {
        let mut is = IntSet::new();
        for v in [5, 1, 9, 3, 7] {
            is.add(v);
        }
        assert_eq!(is.to_vec(), vec![1, 3, 5, 7, 9]);
        assert!(is.remove(5));
        assert!(!is.find(5));
        assert!(!is.remove(5)); // already gone
        assert_eq!(is.to_vec(), vec![1, 3, 7, 9]);
    }

    #[test]
    fn stays_sorted_under_random_inserts() {
        let mut is = IntSet::new();
        let mut state: u64 = 88172645463325252;
        let mut inserted = std::collections::BTreeSet::new();
        for _ in 0..2000 {
            // xorshift64 PRNG, deterministic across runs.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let v = (state % 0x1_0000_0000) as i64 - 0x8000_0000;
            is.add(v);
            inserted.insert(v);
        }
        assert_eq!(is.to_vec(), inserted.into_iter().collect::<Vec<_>>());
    }

    #[test]
    fn to_bytes_from_bytes_round_trip() {
        let mut is = IntSet::new();
        for v in [-70000i64, 1, 2, 65536] {
            is.add(v);
        }
        let blob = is.to_bytes();
        let back = IntSet::from_bytes(&blob).unwrap();
        assert_eq!(is, back);
        assert_eq!(back.to_vec(), vec![-70000, 1, 2, 65536]);
    }

    #[test]
    fn to_bytes_layout_is_little_endian_header() {
        let mut is = IntSet::new();
        is.add(1);
        let blob = is.to_bytes();
        // encoding = 2 (INT16), length = 1, both little-endian u32.
        assert_eq!(&blob[0..4], &[2, 0, 0, 0]);
        assert_eq!(&blob[4..8], &[1, 0, 0, 0]);
        assert_eq!(&blob[8..10], &1i16.to_le_bytes());
    }

    #[test]
    fn from_bytes_rejects_truncated_or_bad_encoding() {
        assert!(IntSet::from_bytes(&[0, 0, 0]).is_none());
        let mut bad = IntSet::new().to_bytes();
        bad[0] = 3; // invalid encoding (must be 2, 4 or 8)
        assert!(IntSet::from_bytes(&bad).is_none());
    }
}
