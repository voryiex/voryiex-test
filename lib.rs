// ============================================================================
// PROJECT: Titan-Vortex Distributed Raft-Backed KV Store (V2.0 Enterprise)
// ADDITIONS: Zero-Copy WAL Replay, Lock-Free Background GC, Read-Only Optimization.
// ============================================================================

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub const MAX_KEY_SIZE: usize = 64;
pub const MAX_VAL_SIZE: usize = 256;
pub const MAX_VERSIONS: usize = 8;
pub const TABLE_CAPACITY: usize = 256;
pub const RAFT_LOG_CAPACITY: usize = 1024;

// ----------------------------------------------------------------------------
// 1. RAFT CONSENSUS & ZERO-COPY WAL REPLAY
// ----------------------------------------------------------------------------
#[repr(C, align(64))]
pub struct RaftLogEntry {
    pub term: AtomicU64,
    pub index: AtomicU64,
    pub key_hash: AtomicU64,
    pub val_len: u32,
    pub data: [u8; MAX_VAL_SIZE],
}

unsafe impl Sync for RaftLogEntry {}

#[repr(C, align(64))]
pub struct RaftReplicationManager {
    pub current_term: AtomicU64,
    pub commit_index: AtomicU64,
    pub log_ring: [RaftLogEntry; RAFT_LOG_CAPACITY],
    pub log_cursor: AtomicU64,
}

impl RaftReplicationManager {
    pub const fn new() -> Self {
        const EMPTY_ENTRY: RaftLogEntry = RaftLogEntry {
            term: AtomicU64::new(0),
            index: AtomicU64::new(0),
            key_hash: AtomicU64::new(0),
            val_len: 0,
            data: [0; MAX_VAL_SIZE],
        };
        Self {
            current_term: AtomicU64::new(1),
            commit_index: AtomicU64::new(0),
            log_ring: [EMPTY_ENTRY; RAFT_LOG_CAPACITY],
            log_cursor: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn append_entry(&self, term: u64, key_hash: u64, val: &[u8]) -> Result<u64, &'static str> {
        let cursor = self.log_cursor.fetch_add(1, Ordering::Relaxed);
        let idx = (cursor as usize) % RAFT_LOG_CAPACITY;
        let entry = &self.log_ring[idx];

        entry.term.store(term, Ordering::Release);
        entry.index.store(cursor + 1, Ordering::Release);
        entry.key_hash.store(key_hash, Ordering::Release);

        let len = core::cmp::min(val.len(), MAX_VAL_SIZE);
        unsafe {
            core::ptr::copy_nonoverlapping(val.as_ptr(), entry.data.as_ptr() as *mut u8, len);
        }
        entry.val_len = len as u32;
        self.commit_index.store(cursor + 1, Ordering::Release);
        Ok(cursor + 1)
    }
}

// ----------------------------------------------------------------------------
// 2. MVCC STORAGE SLOT STRUCTURES
// ----------------------------------------------------------------------------
#[repr(C, align(64))]
pub struct MvccVersionNode {
    pub commit_timestamp: AtomicU64, // 0 = Empty, u64::MAX = Tombstone
    pub active_readers: AtomicU32,
    pub val_len: u32,
    pub value: UnsafeCell<[u8; MAX_VAL_SIZE]>,
}

unsafe impl Sync for MvccVersionNode {}

#[repr(C, align(64))]
pub struct ProductionSlot {
    pub key_hash: AtomicU64,
    pub key_len: u16,
    pub key: UnsafeCell<[u8; MAX_KEY_SIZE]>,
    pub latest_version_idx: AtomicU32,
    pub versions: [MvccVersionNode; MAX_VERSIONS],
}

unsafe impl Sync for ProductionSlot {}

// ----------------------------------------------------------------------------
// 3. TRANSACTION DESCRIPTOR & ROLLBACK
// ----------------------------------------------------------------------------
#[derive(Copy, Clone, PartialEq)]
pub enum TxnOp { Put, Delete }

pub struct TransactionOperation {
    pub op: TxnOp,
    pub key_hash: u64,
    pub key_bytes: [u8; MAX_KEY_SIZE],
    pub key_len: usize,
    pub val_len: u32,
    pub val_bytes: [u8; MAX_VAL_SIZE],
}

pub struct DistributedTransaction {
    pub read_timestamp: u64,
    pub is_aborted: bool,
    pub ops_count: usize,
    pub ops: [TransactionOperation; 4],
}

// ----------------------------------------------------------------------------
// 4. THE TITAN-VORTEX ENGINE (V2.0 WITH GC & REPLAY)
// ----------------------------------------------------------------------------
pub struct TitanVortexEngine {
    pub global_timestamp: AtomicU64,
    pub raft: RaftReplicationManager,
    pub slots: [ProductionSlot; TABLE_CAPACITY],
}

unsafe impl Sync for TitanVortexEngine {}
unsafe impl Send for TitanVortexEngine {}

impl TitanVortexEngine {
    pub const fn new() -> Self {
        const EMPTY_VER: MvccVersionNode = MvccVersionNode {
            commit_timestamp: AtomicU64::new(0),
            active_readers: AtomicU32::new(0),
            val_len: 0,
            value: UnsafeCell::new([0; MAX_VAL_SIZE]),
        };
        const EMPTY_SLOT: ProductionSlot = ProductionSlot {
            key_hash: AtomicU64::new(0),
            key_len: 0,
            key: UnsafeCell::new([0; MAX_KEY_SIZE]),
            latest_version_idx: AtomicU32::new(0),
            versions: [EMPTY_VER; MAX_VERSIONS],
        };

        Self {
            global_timestamp: AtomicU64::new(1),
            raft: RaftReplicationManager::new(),
            slots: [EMPTY_SLOT; TABLE_CAPACITY],
        }
    }

    #[inline(always)]
    pub fn hash_key(key: &[u8]) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in key {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        if h == 0 { 1 } else { h }
    }

    pub fn begin_txn(&self) -> u64 {
        self.global_timestamp.load(Ordering::Acquire)
    }

    pub fn replay_wal(&self) {
        let commit_idx = self.raft.commit_index.load(Ordering::Acquire);
        let limit = core::cmp::min(commit_idx, RAFT_LOG_CAPACITY as u64);
        
        for i in 0..limit {
            let entry = &self.raft.log_ring[i as usize];
            let h = entry.key_hash.load(Ordering::Relaxed);
            if h == 0 { continue; }

            let idx = (h as usize) % TABLE_CAPACITY;
            let slot = &self.slots[idx];
            slot.key_hash.store(h, Ordering::Release);

            let next_v = (slot.latest_version_idx.fetch_add(1, Ordering::AcqRel) as usize + 1) % MAX_VERSIONS;
            let target_ver = &slot.versions[next_v];

            unsafe {
                core::ptr::copy_nonoverlapping(entry.data.as_ptr(), target_ver.value.get() as *mut u8, entry.val_len as usize);
            }
            target_ver.val_len = entry.val_len;
            target_ver.commit_timestamp.store(entry.term.load(Ordering::Relaxed), Ordering::Release);
        }
    }

    pub fn background_gc(&self, min_active_ts: u64) {
        for s in 0..TABLE_CAPACITY {
            let slot = &self.slots[s];
            let latest = slot.latest_version_idx.load(Ordering::Relaxed) as usize % MAX_VERSIONS;

            for v in 0..MAX_VERSIONS {
                if v == latest { continue; }
                let ver = &slot.versions[v];
                let ts = ver.commit_timestamp.load(Ordering::Acquire);

                if ts > 0 && ts < min_active_ts && ts != u64::MAX {
                    if ver.active_readers.load(Ordering::Acquire) == 0 {
                        ver.commit_timestamp.store(0, Ordering::Release);
                        ver.val_len = 0;
                    }
                }
            }
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &[u8], read_ts: u64, out: &mut [u8]) -> Result<usize, &'static str> {
        let h = Self::hash_key(key);
        let idx = (h as usize) % TABLE_CAPACITY;
        let slot = &self.slots[idx];

        if slot.key_hash.load(Ordering::Acquire) != h {
            return Err("Key not found");
        }

        for i in 0..MAX_VERSIONS {
            let ver = &slot.versions[i];
            let ts = ver.commit_timestamp.load(Ordering::Acquire);

            if ts > 0 && ts <= read_ts {
                if ts == u64::MAX { return Err("Key deleted"); }
                ver.active_readers.fetch_add(1, Ordering::Acquire);
                let len = ver.val_len as usize;
                if out.len() < len {
                    ver.active_readers.fetch_sub(1, Ordering::Release);
                    return Err("Buffer too small");
                }
                unsafe {
                    core::ptr::copy_nonoverlapping(ver.value.get() as *const u8, out.as_mut_ptr(), len);
                }
                ver.active_readers.fetch_sub(1, Ordering::Release);
                return Ok(len);
            }
        }
        Err("Snapshot version not found")
    }

    #[inline(always)]
    pub fn commit(&self, txn: &mut DistributedTransaction) -> Result<u64, &'static str> {
        if txn.is_aborted { return Err("Transaction already aborted"); }

        // 1. OCC Validation
        for i in 0..txn.ops_count {
            let op = &txn.ops[i];
            let idx = (op.key_hash as usize) % TABLE_CAPACITY;
            let slot = &self.slots[idx];
            let latest_i = (slot.latest_version_idx.load(Ordering::Relaxed) as usize) % MAX_VERSIONS;
            let latest_ts = slot.versions[latest_i].commit_timestamp.load(Ordering::Acquire);

            if latest_ts > txn.read_timestamp && latest_ts != u64::MAX {
                txn.is_aborted = true;
                return Err("OCC Conflict: Aborted");
            }
        }

        // 2. Raft Log Replication
        let term = self.raft.current_term.load(Ordering::Acquire);
        for i in 0..txn.ops_count {
            let op = &txn.ops[i];
            if op.op == TxnOp::Put {
                let _ = self.raft.append_entry(term, op.key_hash, &op.val_bytes[..op.val_len as usize])?;
            }
        }

        let commit_ts = self.global_timestamp.fetch_add(1, Ordering::SeqCst);

        // 3. Apply State Changes
        for i in 0..txn.ops_count {
            let op = &txn.ops[i];
            let idx = (op.key_hash as usize) % TABLE_CAPACITY;
            let slot = &self.slots[idx];

            slot.key_hash.store(op.key_hash, Ordering::Release);
            unsafe {
                core::ptr::copy_nonoverlapping(op.key_bytes.as_ptr(), slot.key.get() as *mut u8, op.key_len);
            }
            slot.key_len = op.key_len as u16;

            let next_v = (slot.latest_version_idx.fetch_add(1, Ordering::AcqRel) as usize + 1) % MAX_VERSIONS;
            let target_ver = &slot.versions[next_v];

            if target_ver.active_readers.load(Ordering::Acquire) > 0 {
                txn.is_aborted = true;
                return Err("Version Ring Contention: Aborted");
            }

            match op.op {
                TxnOp::Put => {
                    unsafe {
                        core::ptr::copy_nonoverlapping(op.val_bytes.as_ptr(), target_ver.value.get() as *mut u8, op.val_len as usize);
                    }
                    target_ver.val_len = op.val_len;
                    target_ver.commit_timestamp.store(commit_ts, Ordering::Release);
                }
                TxnOp::Delete => {
                    target_ver.val_len = 0;
                    target_ver.commit_timestamp.store(u64::MAX, Ordering::Release);
                }
            }
        }
        Ok(commit_ts)
    }
}
