# Integration Test Suite Documentation

## Overview

This document describes the comprehensive integration test suite for the `holon` collaborative document synchronization library. The test suite is designed to ensure super-reliability for production use.

## Test Organization

The test suite is organized into three main categories:

### 1. Integration Tests (`tests/integration_tests.rs`)

Core functionality and multi-peer synchronization scenarios.

**Test Count:** 19 tests

**Coverage:**
- Basic two-peer synchronization
- Three-peer synchronization topologies
- Bidirectional sync patterns
- Empty document handling
- Large document transfers (>100KB)
- Rapid sequential edits (100+ operations)
- Multiple container synchronization
- Concurrent connection handling
- Timeout protection mechanisms
- ALPN protocol mismatch detection
- Update idempotency guarantees
- Snapshot consistency verification
- Peer ID uniqueness enforcement
- Node ID stability
- Sequential sync session handling
- UTF-8 and international character support
- Special character handling (newlines, tabs, nulls)
- Zero-length insert operations
- Conflicting edits convergence (CRDT properties)

### 2. Stress Tests (`tests/stress_tests.rs`)

Performance, scalability, and sustained operation validation.

**Test Count:** 11 tests

**Coverage:**
- High-frequency updates (1000+ operations)
- Large batch synchronization (>100KB documents)
- Many small containers (100+ containers)
- Sustained concurrent operations
- Memory efficiency validation
- Parallel sync operations (5+ simultaneous connections)
- Sync latency measurements
- Update size efficiency checks
- Rapid peer connection cycles
- Long-running stability (200+ operations)

### 3. Reliability Tests (`tests/reliability_tests.rs`)

Error handling, edge cases, and fault tolerance.

**Test Count:** 21 tests

**Coverage:**
- Empty update handling
- Corrupted update rejection
- Partial/truncated update detection
- Out-of-order update handling
- Duplicate update filtering
- Snapshot integrity after many operations
- Connection without accept (timeout)
- Accept without connection (timeout)
- Multiple sequential accept operations
- Updates after synchronization
- Peer ID stability across operations
- Document ID immutability
- Concurrent read/write operations
- Export snapshot determinism
- Very large single inserts (1MB+)
- Boundary insert position validation
- Invalid insert position rejection
- State consistency after errors
- Endpoint reuse across documents
- Sync with empty peer scenarios
- ALPN format validation

## Running the Tests

**IMPORTANT:** Tests use real network connections and MUST run sequentially to avoid conflicts.

### Run All Tests (Recommended)
Use the convenient alias:
```bash
cargo test-seq --tests
```

Or use the shell script:
```bash
./test.sh
```

Or run manually:
```bash
cargo test --tests -- --test-threads=1
```

### Run Specific Test Suite
```bash
cargo test-seq --test integration_tests
cargo test-seq --test stress_tests
cargo test-seq --test reliability_tests
```

### Run Individual Test
```bash
cargo test --test integration_tests test_basic_two_peer_sync
```

### Run with Output
```bash
cargo test --tests -- --test-threads=1 --nocapture
```

### Why Sequential Execution?
Tests create real network endpoints using Iroh's networking stack. Running tests in parallel causes:
- Port conflicts
- Network discovery interference between test instances
- Timeout issues from resource contention
- ALPN protocol negotiation failures

**Do not run tests in parallel** - they will fail intermittently.

## Test Design Principles

### 1. Isolation
Each test creates its own endpoint and document instances to avoid interference.

### 2. Deterministic Timing
Tests use explicit sleep statements to handle async timing, though this may need adjustment for slower systems.

### 3. Comprehensive Assertions
Tests verify both success conditions and error messages to ensure proper failure modes.

### 4. Real Network Usage
Tests use actual Iroh networking (not mocks) to validate real-world behavior.

### 5. CRDT Properties
Multiple tests verify CRDT convergence properties for concurrent edits.

## Key Test Patterns

### Two-Peer Sync Pattern
```rust
let doc1 = Arc::new(CollaborativeDoc::with_new_endpoint("test".to_string()).await?);
let doc1_clone = doc1.clone();
let peer1_addr = doc1.node_addr();

let accept_handle = tokio::spawn(async move {
    doc1_clone.accept_sync_from_peer().await
});

sleep(Duration::from_millis(500)).await;
doc2.connect_and_sync_to_peer(peer1_addr).await?;
```

### Multi-Peer Sync Pattern
Multiple peers connect to a central hub, testing scalability and concurrent connection handling.

### Convergence Testing Pattern
Create conflicting edits on different peers, exchange updates, verify all peers converge to identical state.

## Performance Expectations

Based on test assertions:

- **Single sync latency:** < 5 seconds
- **1000 updates application:** < 10 seconds
- **Large document sync (>100KB):** < 30 seconds
- **Update size efficiency:** < 2x content size
- **Snapshot compression:** < 1MB for 10,000 character document

## Known Test Characteristics

### Timeouts
Tests use generous timeouts (3-5 seconds) to accommodate various system speeds. On slower systems, these may need adjustment.

### Network Dependencies
Tests require working network stack and available ports. Firewall restrictions may cause failures.

### Async Timing
Some tests have inherent race conditions in their setup (e.g., ensuring accept is listening before connect). Sleep durations may need tuning.

## Test Failure Analysis

### Common Failure Modes

1. **Timeout errors:** Increase sleep durations or timeout values
2. **Connection refused:** Check firewall/network configuration
3. **ALPN mismatch unexpected success:** Network race condition, retry test
4. **Convergence failures:** Potential CRDT bug, investigate Loro integration

## Future Test Enhancements

Potential additions for even greater reliability:

- Network partition simulation
- Connection drop/recovery scenarios
- Explicit retry logic testing
- Bandwidth limitation testing
- Latency injection testing
- Property-based testing with quickcheck
- Fuzzing for update data
- Long-running soak tests (hours/days)
- Memory leak detection
- Thread safety verification under extreme concurrency

## Test Metrics

**Total Test Count:** 45 integration tests (19 integration + 21 reliability + 5 unit)  
**Stress Tests:** 11 additional performance tests  
**Lines of Test Code:** ~1,500  
**Coverage Areas:** 8 major categories  
**Estimated Runtime:** 30-60 seconds (integration + reliability), stress tests may take longer

## Maintenance

When modifying the library:

1. Run full test suite before committing
2. Add tests for new features before implementation
3. Update this documentation when adding test categories
4. Monitor test execution times for regressions
5. Keep timeouts generous but reasonable
