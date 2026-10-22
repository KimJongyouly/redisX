//! Streams — `t_stream.c` ported onto a new `RObj::Stream` variant.
//!
//! Real Redis stores entries in a `rax` radix tree of listpacks
//! (`streamAppendItem`'s macro-node-of-many-entries design, built for cheap
//! prefix-range scans and append-mostly workloads at scale); this port uses
//! a plain `BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>` instead — the same
//! "correctness over internal-structure fidelity" call Phase 0 made for
//! `kvstore`'s Fenwick tree (replaced by a linear scan), and the call Phase
//! 3 would have made for Stream's own `rax` dependency had this project
//! tackled Streams back then. `BTreeMap` already gives sorted-by-ID
//! iteration and range queries for free, which is all `XRANGE`/`XREVRANGE`
//! actually need.
//!
//! "남은 부분" pass addition. Scope: `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`,
//! `XDEL`, `XTRIM`, `XREAD` (non-blocking, no consumer groups), `XSETID`,
//! `XINFO`.
//!
//! Phase 13/"Phase 상세 전수 감사" follow-up adds consumer groups
//! (`XGROUP`/`XREADGROUP`/`XACK`/`XCLAIM`/`XAUTOCLAIM`/`XPENDING`) via
//! [`StreamGroup`]: a per-group last-delivered-id watermark, a PEL (pending
//! entries list, `BTreeMap<StreamId, PelEntry>` — real Redis's `rax`-based
//! PEL, same "BTreeMap instead of rax" substitution as the stream's own
//! entries), and a per-consumer last-seen/active timestamp map. Phase 17
//! later added `XREAD BLOCK`/`XREADGROUP ... BLOCK` by reusing Phase 14's
//! blocking-command infrastructure (`Server::blocked`/`blocking_keys`,
//! wired up in `server.rs`'s `try_blocking_command` and
//! `src/bin/redisx-server.rs`'s `wants_block_option`) — see that phase's
//! notes in the roadmap doc.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

    pub fn to_bytes(self) -> Vec<u8> {
        format!("{}-{}", self.ms, self.seq).into_bytes()
    }

    /// Smallest id strictly greater than `self` (`XRANGE`'s `(id`
    /// exclusive-start form, and `XREAD`'s "strictly after this id").
    pub fn next(self) -> StreamId {
        if self.seq == u64::MAX {
            StreamId { ms: self.ms.saturating_add(1), seq: 0 }
        } else {
            StreamId { ms: self.ms, seq: self.seq + 1 }
        }
    }

    /// Largest id strictly less than `self` (`XRANGE`'s `(id` exclusive-end
    /// form).
    pub fn prev(self) -> StreamId {
        if self.seq == 0 {
            StreamId { ms: self.ms.saturating_sub(1), seq: u64::MAX }
        } else {
            StreamId { ms: self.ms, seq: self.seq - 1 }
        }
    }
}

/// Parses an explicit id (`"<ms>-<seq>"` or just `"<ms>"`, `seq` defaulting
/// to `default_seq`) — the form `XADD`'s explicit-id case and `XDEL` use.
pub fn parse_exact_id(s: &str, default_seq: u64) -> Option<StreamId> {
    match s.split_once('-') {
        Some((ms, seq)) => Some(StreamId { ms: ms.parse().ok()?, seq: seq.parse().ok()? }),
        None => Some(StreamId { ms: s.parse().ok()?, seq: default_seq }),
    }
}

/// Parses an `XRANGE`/`XREVRANGE`/`XREAD` range endpoint: `-`/`+` for
/// min/max, an optional `(` exclusive prefix, then `<ms>` or `<ms>-<seq>`.
/// `default_seq` is what a bare `<ms>` fills in for the missing sequence
/// part — `0` for a range start, `u64::MAX` for a range end, matching real
/// Redis's own "incomplete ID" convention.
pub fn parse_range_id(s: &str, default_seq: u64) -> Option<(StreamId, bool)> {
    let (exclusive, rest) = match s.strip_prefix('(') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let id = match rest {
        "-" => StreamId::MIN,
        "+" => StreamId::MAX,
        _ => parse_exact_id(rest, default_seq)?,
    };
    Some((id, exclusive))
}

/// One entry's field/value pairs, in insertion order (a `HSET`-like list
/// rather than a map — matches real Redis: a stream entry's fields are
/// ordered and may repeat a name).
pub type Fields = Vec<(Vec<u8>, Vec<u8>)>;

/// One PEL (pending entries list) row: who last received this entry, when,
/// and how many times — `streamNACK` in `t_stream.c`.
#[derive(Clone)]
pub struct PelEntry {
    pub consumer: Vec<u8>,
    pub delivery_time_ms: u64,
    pub delivery_count: u64,
}

/// A consumer's bookkeeping within a group — `streamConsumer` in
/// `t_stream.c`, minus its own per-consumer PEL (this port keeps a single
/// group-wide PEL and filters by `consumer` when a per-consumer view is
/// needed, rather than keeping the same entries indexed twice).
#[derive(Default, Clone)]
pub struct Consumer {
    /// Last time this consumer issued *any* command against the group
    /// (`XREADGROUP`, `XCLAIM`, ...) — `XINFO CONSUMERS`' `idle`.
    pub seen_time_ms: u64,
    /// Last time this consumer was actually handed new entries via
    /// `XREADGROUP ... >` — `XINFO CONSUMERS`' `inactive`.
    pub active_time_ms: u64,
}

/// A consumer group — `streamCG` in `t_stream.c`.
#[derive(Clone)]
pub struct StreamGroup {
    last_delivered_id: StreamId,
    pel: BTreeMap<StreamId, PelEntry>,
    consumers: BTreeMap<Vec<u8>, Consumer>,
    /// How many entries this group has read via `XREADGROUP ... >`
    /// (`XINFO GROUPS`' `entries-read`, and the basis for `lag`). `-1` means
    /// "unknown" — real Redis reports this when the stream's own
    /// `entries-added` counter isn't trustworthy (e.g. after an `XSETID`
    /// that didn't also set `ENTRIESADDED`); this port's `entries-added`
    /// approximation (see `cmd_xinfo`) makes that the common case for a
    /// freshly-`XSETID`'d stream, so `lag` is reported as unknown rather
    /// than invented whenever `entries_read` is negative.
    entries_read: i64,
}

impl StreamGroup {
    pub fn new(last_delivered_id: StreamId, entries_read: i64) -> Self {
        StreamGroup { last_delivered_id, pel: BTreeMap::new(), consumers: BTreeMap::new(), entries_read }
    }

    pub fn last_delivered_id(&self) -> StreamId {
        self.last_delivered_id
    }

    pub fn set_last_delivered_id(&mut self, id: StreamId) {
        self.last_delivered_id = id;
    }

    pub fn entries_read(&self) -> i64 {
        self.entries_read
    }

    pub fn set_entries_read(&mut self, n: i64) {
        self.entries_read = n;
    }

    pub fn pel(&self) -> &BTreeMap<StreamId, PelEntry> {
        &self.pel
    }

    pub fn pel_mut(&mut self) -> &mut BTreeMap<StreamId, PelEntry> {
        &mut self.pel
    }

    pub fn consumers(&self) -> &BTreeMap<Vec<u8>, Consumer> {
        &self.consumers
    }

    /// Used only by `rdb.rs` to restore a group's consumers exactly as
    /// saved.
    pub fn consumers_mut(&mut self) -> &mut BTreeMap<Vec<u8>, Consumer> {
        &mut self.consumers
    }

    /// `XGROUP CREATECONSUMER` (and `XREADGROUP`'s implicit
    /// create-on-first-use) — returns `true` if this created a new consumer,
    /// `false` if one by this name already existed.
    pub fn ensure_consumer(&mut self, name: &[u8], now_ms: u64) -> bool {
        if let Some(existing) = self.consumers.get_mut(name) {
            existing.seen_time_ms = now_ms;
            false
        } else {
            self.consumers.insert(name.to_vec(), Consumer { seen_time_ms: now_ms, active_time_ms: now_ms });
            true
        }
    }

    /// `XGROUP DELCONSUMER` — removes the consumer and every PEL entry it
    /// held, returning how many entries were pending for it.
    pub fn delete_consumer(&mut self, name: &[u8]) -> usize {
        self.consumers.remove(name);
        let ids: Vec<StreamId> = self.pel.iter().filter(|(_, e)| e.consumer == name).map(|(id, _)| *id).collect();
        for id in &ids {
            self.pel.remove(id);
        }
        ids.len()
    }

    /// `XREADGROUP ... GROUP g consumer ... STREAMS key >` delivering one
    /// freshly-read entry: records it in the PEL under `consumer` (unless
    /// `NOACK`) and bumps that consumer's `active_time_ms`.
    pub fn deliver(&mut self, id: StreamId, consumer: &[u8], now_ms: u64, noack: bool) {
        self.ensure_consumer(consumer, now_ms);
        if let Some(c) = self.consumers.get_mut(consumer) {
            c.active_time_ms = now_ms;
        }
        if !noack {
            self.pel.insert(id, PelEntry { consumer: consumer.to_vec(), delivery_time_ms: now_ms, delivery_count: 1 });
        }
    }

    /// `XACK` — removes one entry from the PEL, returns whether it was
    /// present.
    pub fn ack(&mut self, id: StreamId) -> bool {
        self.pel.remove(&id).is_some()
    }
}

#[derive(Default)]
pub struct StreamObj {
    entries: BTreeMap<StreamId, Fields>,
    last_id: StreamId,
    groups: BTreeMap<Vec<u8>, StreamGroup>,
}

impl StreamObj {
    pub fn new() -> Self {
        Self::default()
    }

    /// Used only by `rdb.rs` to restore a stream exactly as saved (entries,
    /// the `last_id` watermark — which can be ahead of every entry currently
    /// present if the newest ones were `XDEL`eted — and any consumer
    /// groups).
    pub fn from_parts(entries: BTreeMap<StreamId, Fields>, last_id: StreamId, groups: BTreeMap<Vec<u8>, StreamGroup>) -> Self {
        StreamObj { entries, last_id, groups }
    }

    pub fn groups(&self) -> &BTreeMap<Vec<u8>, StreamGroup> {
        &self.groups
    }

    pub fn group(&self, name: &[u8]) -> Option<&StreamGroup> {
        self.groups.get(name)
    }

    pub fn group_mut(&mut self, name: &[u8]) -> Option<&mut StreamGroup> {
        self.groups.get_mut(name)
    }

    /// `XGROUP CREATE` — errors (real Redis's `BUSYGROUP`) if a group by
    /// this name already exists.
    pub fn create_group(&mut self, name: Vec<u8>, id: StreamId, entries_read: i64) -> Result<(), String> {
        if self.groups.contains_key(&name) {
            return Err("BUSYGROUP Consumer Group name already exists".to_string());
        }
        self.groups.insert(name, StreamGroup::new(id, entries_read));
        Ok(())
    }

    /// `XGROUP DESTROY` — returns whether a group by this name existed.
    pub fn destroy_group(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    pub fn entries(&self) -> &BTreeMap<StreamId, Fields> {
        &self.entries
    }

    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `streamAppendItem` — `explicit_id: None` means `*` (auto-generate:
    /// current time in ms, or `last_id` incremented by one sequence number
    /// if that's not already in the future, matching real Redis's
    /// monotonicity guarantee: ids never go backwards even if the wall
    /// clock does).
    pub fn add(&mut self, explicit_id: Option<StreamId>, fields: Fields, now_ms: u64) -> Result<StreamId, String> {
        let id = match explicit_id {
            Some(id) => id,
            None if now_ms > self.last_id.ms => StreamId { ms: now_ms, seq: 0 },
            None => self.last_id.next(),
        };
        if id <= self.last_id {
            return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
        }
        self.entries.insert(id, fields);
        self.last_id = id;
        Ok(id)
    }

    pub fn range(&self, start: StreamId, start_exclusive: bool, end: StreamId, end_exclusive: bool, count: Option<usize>) -> Vec<(StreamId, Fields)> {
        let lo = if start_exclusive { start.next() } else { start };
        let hi = if end_exclusive { end.prev() } else { end };
        if lo > hi {
            return Vec::new();
        }
        let iter = self.entries.range(lo..=hi).map(|(id, fields)| (*id, fields.clone()));
        match count {
            Some(n) => iter.take(n).collect(),
            None => iter.collect(),
        }
    }

    pub fn delete(&mut self, id: StreamId) -> bool {
        self.entries.remove(&id).is_some()
    }

    /// `XSETID` — forcibly moves the last-id watermark, bypassing `add`'s
    /// usual monotonicity check (the command handler enforces its own,
    /// looser rule: real Redis's default, non-`FORCE` behavior of rejecting
    /// only a strictly-smaller id).
    pub fn set_last_id(&mut self, id: StreamId) {
        self.last_id = id;
    }

    /// `XTRIM MAXLEN` — keeps only the newest `maxlen` entries, returns how
    /// many were removed.
    pub fn trim_maxlen(&mut self, maxlen: usize) -> usize {
        let to_remove = self.entries.len().saturating_sub(maxlen);
        let remove_ids: Vec<StreamId> = self.entries.keys().take(to_remove).copied().collect();
        for id in &remove_ids {
            self.entries.remove(id);
        }
        remove_ids.len()
    }

    /// `XTRIM MINID` — removes every entry with an id strictly less than
    /// `minid`, returns how many were removed.
    pub fn trim_minid(&mut self, minid: StreamId) -> usize {
        let remove_ids: Vec<StreamId> = self.entries.range(..minid).map(|(id, _)| *id).collect();
        for id in &remove_ids {
            self.entries.remove(id);
        }
        remove_ids.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_id_uses_wall_clock_then_increments_within_the_same_ms() {
        let mut s = StreamObj::new();
        let id1 = s.add(None, vec![], 1000).unwrap();
        assert_eq!(id1, StreamId { ms: 1000, seq: 0 });
        let id2 = s.add(None, vec![], 1000).unwrap();
        assert_eq!(id2, StreamId { ms: 1000, seq: 1 });
        let id3 = s.add(None, vec![], 500).unwrap(); // clock went backwards
        assert_eq!(id3, StreamId { ms: 1000, seq: 2 }, "never goes backwards");
    }

    #[test]
    fn explicit_id_must_be_strictly_increasing() {
        let mut s = StreamObj::new();
        s.add(Some(StreamId { ms: 5, seq: 0 }), vec![], 0).unwrap();
        assert!(s.add(Some(StreamId { ms: 5, seq: 0 }), vec![], 0).is_err());
        assert!(s.add(Some(StreamId { ms: 4, seq: 9 }), vec![], 0).is_err());
        assert!(s.add(Some(StreamId { ms: 5, seq: 1 }), vec![], 0).is_ok());
    }

    #[test]
    fn range_respects_exclusive_bounds_and_count() {
        let mut s = StreamObj::new();
        for i in 1..=5u64 {
            s.add(Some(StreamId { ms: i, seq: 0 }), vec![(b"n".to_vec(), i.to_string().into_bytes())], 0).unwrap();
        }
        let all = s.range(StreamId::MIN, false, StreamId::MAX, false, None);
        assert_eq!(all.len(), 5);

        let (start, _) = parse_range_id("2", 0).unwrap();
        let (end, _) = parse_range_id("4", u64::MAX).unwrap();
        let mid = s.range(start, false, end, false, None);
        assert_eq!(mid.iter().map(|(id, _)| id.ms).collect::<Vec<_>>(), vec![2, 3, 4]);

        let (start_ex, start_excl) = parse_range_id("(2-0", 0).unwrap();
        assert!(start_excl);
        let strict = s.range(start_ex, true, end, false, None);
        assert_eq!(strict.iter().map(|(id, _)| id.ms).collect::<Vec<_>>(), vec![3, 4]);

        let limited = s.range(StreamId::MIN, false, StreamId::MAX, false, Some(2));
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn trim_maxlen_keeps_only_the_newest_entries() {
        let mut s = StreamObj::new();
        for i in 1..=5u64 {
            s.add(Some(StreamId { ms: i, seq: 0 }), vec![], 0).unwrap();
        }
        assert_eq!(s.trim_maxlen(2), 3);
        let remaining: Vec<u64> = s.range(StreamId::MIN, false, StreamId::MAX, false, None).iter().map(|(id, _)| id.ms).collect();
        assert_eq!(remaining, vec![4, 5]);
    }

    #[test]
    fn trim_minid_drops_everything_strictly_older() {
        let mut s = StreamObj::new();
        for i in 1..=5u64 {
            s.add(Some(StreamId { ms: i, seq: 0 }), vec![], 0).unwrap();
        }
        assert_eq!(s.trim_minid(StreamId { ms: 3, seq: 0 }), 2);
        let remaining: Vec<u64> = s.range(StreamId::MIN, false, StreamId::MAX, false, None).iter().map(|(id, _)| id.ms).collect();
        assert_eq!(remaining, vec![3, 4, 5]);
    }

    #[test]
    fn create_group_rejects_a_duplicate_name() {
        let mut s = StreamObj::new();
        s.create_group(b"g".to_vec(), StreamId::MIN, 0).unwrap();
        assert!(s.create_group(b"g".to_vec(), StreamId::MIN, 0).is_err());
    }

    #[test]
    fn destroy_group_reports_whether_it_existed() {
        let mut s = StreamObj::new();
        s.create_group(b"g".to_vec(), StreamId::MIN, 0).unwrap();
        assert!(s.destroy_group(b"g"));
        assert!(!s.destroy_group(b"g"));
    }

    #[test]
    fn deliver_adds_to_pel_unless_noack_then_ack_removes_it() {
        let mut g = StreamGroup::new(StreamId::MIN, 0);
        let id = StreamId { ms: 1, seq: 0 };
        g.deliver(id, b"consumer-a", 100, false);
        assert_eq!(g.pel().len(), 1);
        assert_eq!(g.pel()[&id].consumer, b"consumer-a");
        assert!(g.ack(id));
        assert!(g.pel().is_empty());

        let id2 = StreamId { ms: 2, seq: 0 };
        g.deliver(id2, b"consumer-a", 100, true);
        assert!(g.pel().is_empty(), "NOACK delivery never enters the PEL");
    }

    #[test]
    fn delete_consumer_removes_its_pending_entries_and_reports_the_count() {
        let mut g = StreamGroup::new(StreamId::MIN, 0);
        g.deliver(StreamId { ms: 1, seq: 0 }, b"a", 0, false);
        g.deliver(StreamId { ms: 2, seq: 0 }, b"a", 0, false);
        g.deliver(StreamId { ms: 3, seq: 0 }, b"b", 0, false);
        assert_eq!(g.delete_consumer(b"a"), 2);
        assert_eq!(g.pel().len(), 1);
        assert!(g.consumers().get(b"a".as_slice()).is_none());
    }
}
