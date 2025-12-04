# Rusty Knowledge - Iroh + Loro Integration

A Rust project demonstrating the integration of Iroh (peer-to-peer networking) and Loro (CRDT library) for distributed collaborative applications.

## 🚀 Quick Start - Tauri MVP

**NEW**: A desktop task management UI is now available! To run the Tauri application:

```bash
./run-app.sh
```

Or manually:
```bash
npm install
npm run tauri dev
```

See [docs/tauri-mvp.md](docs/tauri-mvp.md) for details about the MVP features and architecture.

## Overview

This project showcases how to combine:
- **Iroh v0.93.2**: High-performance P2P networking with QUIC-based transport
- **Loro v1.0**: CRDT (Conflict-free Replicated Data Type) library for collaborative editing

## Architecture

The `CollaborativeDoc` struct in `src/sync.rs` provides:

1. **CRDT Document Management**: Each instance maintains a Loro document with collaborative text editing capabilities
2. **P2P Networking**: Uses Iroh's endpoint for secure peer-to-peer communication with DNS discovery (n0)
3. **Document-level Isolation**: Uses document IDs and ALPN to route different documents separately
4. **Update Synchronization**: Exports and applies CRDT updates between peers

## Key Concepts: Two Types of Identity

This implementation requires understanding **two separate identities**:
- **Discovery**: Uses Iroh's DNS discovery service (n0) for automatic peer discovery

## How Peer Discovery Works

When you create a `CollaborativeDoc`, it automatically:

1. **Generates a unique Node ID** (Ed25519 public key) that identifies this peer globally
2. **Registers with discovery service** (n0 DNS) so other peers can find it
3. **Listens for connections** on QUIC endpoints

To connect two computers:

1. **Computer A** runs the application and gets a Node ID
2. **Computer B** runs the application with Computer A's Node ID
3. **Iroh automatically discovers** the connection path (direct, relay, or NAT traversal)
4. **CRDT synchronization** happens over the secure QUIC connection

## Practical Example

Run the peer discovery example to see two peers connect and sync:

### On Computer 1 (Server):
```bash
cargo run --example peer_discovery server
```

This will output:
```
=== Server Mode ===
Creating collaborative document...
✅ Server ready!
📋 Node ID: <some-node-id>

Share this Node ID with the client to connect.
Waiting for connections...
```

### On Computer 2 (Client):
```bash
cargo run --example peer_discovery client <node-id-from-server>
```

The client will:
1. Create its own collaborative document
2. Add some text locally
3. Connect to the server using the Node ID
4. Send its snapshot to sync with the server

Both peers will now have their CRDT documents synchronized!

## Programmatic Usage
### 1. Node ID (Network Identity)
- **What**: A cryptographic public key (Ed25519) that identifies a peer on the network
- **Purpose**: "Who am I?" - Used for authentication and discovery
- **Scope**: One per Iroh endpoint (you can have multiple documents on one endpoint)
- **Example**: `a3f2b8c...` (64 hex characters)

### 2. Document ID (Collaboration Scope)
- **What**: A string identifier for a specific collaborative document/topic
- **Purpose**: "Which document are we editing?" - Determines which peers collaborate together
- **Scope**: Multiple documents can exist on the same endpoint
- **Example**: `"project-notes"`, `"team-todo"`, `"design-doc"`

### Why Both?

Without document IDs, you couldn't have:
- Multiple isolated collaborative documents on the same computer
- Selective sharing (share document A with person X, document B with person Y)
- Topic-based routing (different documents use different ALPN protocols)

**The Rule**: Two peers can only collaborate if they:
1. Know each other's **Node ID** (for discovery)
2. Use the **same Document ID** (for routing)

## How Peer Discovery Works

When you create a `CollaborativeDoc`:

1. **Share the Endpoint**: Multiple documents can share one Iroh endpoint
2. **Specify Document ID**: Each document gets a unique identifier
3. **ALPN Routing**: Connections use `loro-sync/{doc_id}` as the ALPN protocol
4. **Automatic Validation**: Peers verify they're connecting to the correct document

### Discovery Flow

1. **Computer A** creates endpoint and document `"project-notes"`
   - Gets Node ID: `abc123...`
   - Registers with n0 discovery service
   
2. **Computer B** creates endpoint and document `"project-notes"` (same ID!)
   - Connects using Computer A's Node ID
   - Iroh resolves the connection path
   - ALPN ensures they're syncing the same document
   
3. **Computer C** creates document `"other-doc"` (different ID)
   - Even if it knows Node ID of A or B
   - Cannot sync because ALPN doesn't match
   - Documents remain isolated

## Practical Example

The example demonstrates true **symmetric P2P** - there's no client/server distinction! Every peer can both send and receive connections.

### First Peer (will wait for connections):
```bash
cargo run --example peer_discovery project-notes
```

Output:
```
=== Starting P2P Peer ===
📄 Document: 'project-notes'

✅ Peer ready!
📋 My Node ID: fcdd5715c6242291cdd08255d1d1a19d99eb48adb126c1070f024904bfc20668

📝 Initial text: Hello from peer fcdd5715!

⏳ Waiting for other peers to connect...
   Share this command with other peers:
   cargo run --example peer_discovery project-notes fcdd5715c6242291...
```

### Second Peer (connects to first peer):
```bash
cargo run --example peer_discovery project-notes fcdd5715c6242291...
```

Output:
```
=== Starting P2P Peer ===
📄 Document: 'project-notes'

✅ Peer ready!
📋 My Node ID: 8f22fa61f65f8b9351af01af9fe3830314b773e24c4f6ae00948406d4f1956da

📝 Initial text: Hello from peer 8f22fa61!

🔗 Connecting to peer: fcdd5715c6242291...
   (I can still accept connections from others too!)

✅ Sync complete!
📝 After sync: Hello from peer 8f22fa61!
```

Both peers will have their CRDT documents synchronized! The second peer can also accept connections from a third peer while staying connected.

### Key Points:

1. **Symmetric**: Both peers use the same code path - just pass a peer ID to connect, or don't to wait
2. **Bi-directional**: Every peer can both initiate and accept connections
3. **Multi-peer**: Each peer can connect to multiple other peers (mesh network)
4. **Document isolation**: Only peers with matching document IDs can sync

### What Happens with Wrong Document ID?

If a peer uses a different document ID:
```bash
cargo run --example peer_discovery wrong-doc fcdd5715c6242291...
```

The connection will **fail** with:
```
Error: Wrong document! Expected 'project-notes' but got ALPN: 'loro-sync/wrong-doc'
```

## Programmatic Usage

### Simple: One Document Per Peer

```rust
use holon::sync::CollaborativeDoc;

// Create a document with its own endpoint (easiest way)
let doc = CollaborativeDoc::with_new_endpoint("project-notes".to_string()).await?;

// Add content
doc.insert_text("editor", 0, "Hello World").await?;

// Connect to a peer
let peer_node_id: iroh::PublicKey = "abc123...".parse()?;
let peer_addr = iroh::NodeAddr::new(peer_node_id);
doc.connect_and_sync_to_peer(peer_addr).await?;

// Or wait for connections
doc.accept_sync_from_peer().await?;
```

### Advanced: Multiple Documents Sharing One Endpoint

```rust
use holon::sync::CollaborativeDoc;
use std::sync::Arc;

// Create ONE endpoint that can handle multiple documents
// Note: All documents must use the same doc_id for ALPN to work properly
let endpoint = CollaborativeDoc::create_endpoint("shared-workspace").await?;

// Create multiple documents on the same endpoint
let notes = CollaborativeDoc::new(endpoint.clone(), "shared-workspace".to_string()).await?;
let tasks = CollaborativeDoc::new(endpoint.clone(), "shared-workspace".to_string()).await?;

// Use different containers to separate content
notes.insert_text("notes-container", 0, "Notes here").await?;
tasks.insert_text("tasks-container", 0, "Tasks here").await?;
```

cargo test --tests -- --test-threads=1
```

**IMPORTANT**: Tests use real network connections and must run sequentially. Use one of:
- `cargo test-seq --tests` (recommended - uses cargo alias)
- `./test.sh` (shell script wrapper)
- `cargo test --tests -- --test-threads=1` (manual)

See [TESTING.md](TESTING.md) for detailed testing documentation.

## Tests Included

1. `test_create_collaborative_doc` - Basic document creation with ID
2. `test_text_operations` - Text insertion and retrieval
3. `test_update_export_and_apply` - Update synchronization between two documents
4. `test_concurrent_edits_merge` - Concurrent edits with automatic merging
5. `test_different_documents_isolated` - Verifies documents with different IDs don't interfere

## Discovery Mechanisms

The current implementation uses **DNS discovery via n0**, which:
- Works across the internet (not just LAN)
- Automatically handles NAT traversal
- Uses relay servers when direct connection isn't possible
- Is operated by the Iroh team

### Alternative Discovery Options

You can also enable:
- **Local Network Discovery** (mDNS): For LAN-only discovery
- **DHT Discovery**: Fully decentralized using BitTorrent mainline DHT

See [Iroh Discovery Documentation](https://www.iroh.computer/docs/concepts/discovery) for more details.

## How It Works - Complete Picture

1. **Endpoint Creation**: One Iroh endpoint per process (can be shared)
   - Generates Node ID (cryptographic identity)
   - Registers with discovery service
   
2. **Document Creation**: Multiple documents per endpoint
   - Each gets a Document ID
   - Each uses ALPN: `loro-sync/{doc_id}`
   
3. **Peer Discovery**: Node ID → Network Location
   - Discovery service resolves Node IDs to addresses
   - Works across internet, handles NAT traversal
   
4. **Connection**: Secure QUIC connection
   - Authenticated by Node IDs (public keys)
   - Routed by Document ID (via ALPN)
   
5. **Synchronization**: CRDT merge
   - Only matching document IDs sync
   - Loro ensures eventual consistency
   - Updates are incremental

## Discovery Mechanisms

The current implementation uses **DNS discovery via n0**, which:
- Works across the internet (not just LAN)
- Automatically handles NAT traversal
- Uses relay servers when direct connection isn't possible
- Is operated by the Iroh team

### Alternative Discovery Options

You can also enable:
- **Local Network Discovery** (mDNS): For LAN-only discovery
- **DHT Discovery**: Fully decentralized using BitTorrent mainline DHT

See [Iroh Discovery Documentation](https://www.iroh.computer/docs/concepts/discovery) for more details.

## Dependencies

- `iroh` v0.93.293.2 - P2P networking
- `loro` v1.0 - CRDT implementation
- `tokio` - Async runtime
- `anyhow` - Error handling
- `tracing` - Logging

## Architecture Benefits

This two-level identity system enables:
- ✅ Multiple collaborative documents per peer
- ✅ Selective document sharing
- ✅ Isolation between unrelated documents
- ✅ Efficient routing via ALPN
- ✅ True peer-to-peer without central coordination
- ✅ Cryptographic authentication built-in

Perfect for building local-first, collaborative applications!
