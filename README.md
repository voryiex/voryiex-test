# VortexTest

`VortexTest` is a high-performance, experimental in-memory key-value engine built from scratch in pure Rust. It serves as an architectural testbed and prototype for exploring low-level systems concepts, concurrency, and distributed consensus mechanisms.

> **Note / Disclaimer:** 
> This repository is a proof-of-concept / prototype testbed. It is designed for educational, research, and experimentation purposes in systems engineering, and it is not intended for heavy production use in its current form.

---

## Core Architecture and Features

* **MVCC (Multi-Version Concurrency Control):** Lock-free version management with optimistic concurrency control (OCC) validation and tombstone support.
* **Raft-Backed Replication:** Custom zero-copy Raft log ring structure designed for consensus state tracking and replication simulation.
* **Zero-Copy WAL Replay:** Fast recovery mechanism to rebuild state instantly from transaction logs upon initialization.
* **Lock-Free Garbage Cleaning:** Background reclamation of outdated version nodes without stopping active readers.
* **Cache-Aligned Layouts:** Explicit memory alignment (`#[repr(C, align(64))]`) and atomic operations to optimize CPU cache lines and avoid data races.

---
