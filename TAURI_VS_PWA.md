# Tauri vs PWA: Technical Comparison

## Loro CRDT Browser Support

### Platform Support

Loro CRDT library works across multiple platforms:

- **Core**: Written in Rust
- **Browser**: JavaScript/TypeScript bindings via WebAssembly (npm package: `loro-crdt`)
- **Native**: Direct Rust API
- **Mobile**: Swift support for iOS/macOS (as of v1.0)

### Browser Usage

```javascript
import { Loro } from "loro-crdt";

const doc = new Loro();
doc.getText("text").insert(0, "Hello!");
// CRDT operations work identically in browser and native environments
```

### Key Features

- Automatic conflict resolution
- Offline support and history replay
- Data structures: text, lists, trees, maps
- Rich text CRDT with concurrent edit merging
- Version control semantics (Git-like)
- P2P synchronization with delta updates

## Browser State Persistence Options

### Storage Technologies Comparison

| Storage | Capacity | Performance | Persistence | Notes |
|---------|----------|-------------|-------------|-------|
| **IndexedDB** | High (~GB) | Moderate | Survives reload | Most common for CRDTs, transactional |
| **OPFS** | High | High | Survives reload | Origin Private File System, newer standard |
| **SQLite WASM** | High | High | Survives reload | Full SQL database in browser |
| **LocalStorage** | ~5MB | Fast (small data) | Survives reload | Too limited for CRDT state |
| **SessionStorage** | Low | Fast | Cleared on close | Not persistent |

### How CRDT Libraries Handle Browser Storage

**Automerge**:
- Provides `StorageAdapter` abstraction
- Built-in IndexedDB adapter: `@automerge/automerge-repo-storage-indexeddb`
- Safe for concurrent access from multiple tabs
- Supports custom adapters for OPFS or SQLite WASM

**Yjs**:
- Uses IndexedDB via `y-indexeddb` connector
- Supports custom storage integrations
- IndexedDB remains default in web environments

**Loro**:
- Similar architecture expected
- IndexedDB typical default
- Custom adapter implementations possible

### OPFS (Origin Private File System)

**Availability**: Chrome, Edge, Safari (preview), Firefox (in progress)

**Characteristics**:
- Part of File System Access API
- Better concurrency handling than IndexedDB
- Lower latency for read/write operations
- More robust consistency guarantees
- Native-like file semantics
- Works well with SQLite WASM

**Adoption status**: Increasing in CRDT frameworks but less common than IndexedDB as of 2025

## SQL-Like Functionality in Browser

### Available Solutions

| Solution | Storage Backend | Performance | Characteristics |
|----------|----------------|-------------|-----------------|
| **wa-sqlite** | OPFS (primary) or IndexedDB | High | Modern WASM build, OPFS support, active maintenance |
| **electric-sql** | OPFS + cloud sync | High | Built on wa-sqlite/cr-sqlite, CRDT support, Postgres-compatible subset |
| **cr-sqlite** | OPFS via wa-sqlite | High | Extends SQLite with CRDTs, designed for real-time sync |
| **absurd-sql** | IndexedDB emulation | Moderate | Maps SQLite to IndexedDB, larger dataset support than sql.js |
| **sql.js** | In-memory only | Low-Moderate | Entire database in RAM, no default persistence |

### OPFS vs IndexedDB for SQLite

**OPFS advantages**:
- Nearly native file system performance
- Better for write-heavy workloads
- Direct file access semantics
- Lower I/O overhead

**IndexedDB characteristics**:
- Broader browser support
- More complex I/O path when emulating file system
- Higher latency compared to OPFS
- Important for fallback/legacy support

### wa-sqlite Example

```javascript
import SQLiteESMFactory from 'wa-sqlite';
import * as SQLite from 'wa-sqlite';

const sqlite3 = await SQLiteESMFactory();
const db = await sqlite3.open_v2('mydb.db');
// Full SQLite API available
```

## Architectural Options for Offline-First Apps with CRDT + SQL

### Option 1: Tauri (Native Desktop)

**Architecture**:
```
Frontend (TypeScript/JS)
├── Loro CRDT (WASM in webview)
├── HTTP calls via fetch
└── IPC to Rust backend

Rust Backend
├── Loro CRDT (native Rust)
├── SQLite (rusqlite) for caching/queries
├── HTTP client (reqwest) for API calls
└── File system for CRDT persistence
```

**Technical characteristics**:
- Native file system access (no browser quota limits)
- Full SQLite via rusqlite (native performance)
- Background processes possible
- File watching and OS integration available
- Single platform binary per OS

**Constraints**:
- Desktop-only (Windows, macOS, Linux)
- No mobile support
- Distribution requires download/installation
- Updates require new binary (or auto-update implementation)

**Storage**:
- CRDT state: Files on disk
- SQL database: Native SQLite file
- No browser storage limitations
- No quota management needed

### Option 2: PWA (Web-First)

**Architecture**:
```
Frontend (TypeScript/JS)
├── Loro CRDT (WASM)
├── IndexedDB/OPFS for CRDT persistence
├── wa-sqlite (WASM) for caching/queries
├── HTTP calls via fetch
└── Service worker for offline
```

**Technical characteristics**:
- Cross-platform (any device with modern browser)
- Zero installation friction
- Instant updates
- URL-based distribution
- IndexedDB or OPFS for storage

**Constraints**:
- Browser storage quotas apply
- iOS PWA limitations (see PWA support table below)
- No background processes (limited service workers only)
- No deep OS integration
- Rust code requires WASM compilation

**Storage**:
- CRDT state: IndexedDB or OPFS
- SQL database: wa-sqlite backed by OPFS/IndexedDB
- Must handle quota exceeded errors
- Storage persistence not guaranteed (browser may evict data)

### Option 3: Hybrid Approach

**Architecture**:
```
Core Logic (Rust)
├── Loro CRDT
├── SQLite queries
├── HTTP API client logic
└── Compiled to:
    ├── Native (Tauri)
    └── WASM (PWA)

Web Version:
├── Rust → WASM
├── wa-sqlite via WASM
├── OPFS for storage

Desktop Version (Tauri):
├── Native Rust
├── Native SQLite
├── File system storage
```

**Technical characteristics**:
- Code reuse between platforms
- Optimal performance on each platform
- Supports both web and desktop deployment

**Constraints**:
- Two storage systems to manage
- Different APIs for file system (OPFS vs native)
- Increased testing surface (web + desktop)
- More complex build pipeline
- Need to handle platform differences in core logic

## PWA Platform Support Summary (2025)

| Platform | Support Level | Capabilities | Limitations |
|----------|---------------|--------------|-------------|
| **Windows** | Full | Install prompts, notifications, offline, hardware APIs | Minor differences vs native apps |
| **macOS** | Full | Chromium browsers excellent, Safari good | Safari slightly behind on some hardware APIs |
| **Linux** | Full | Wide browser support | Varies by distribution/browser |
| **Android** | Excellent | Near-native parity, push notifications, background sync | Rare performance gaps vs native |
| **iOS** | Partial | Basic functionality, some notifications | No install prompts, limited background sync, reduced storage, restricted hardware access (no Bluetooth, NFC), Safari-only |

### iOS Specific Limitations

- No automatic install prompts (manual "Add to Home Screen" required)
- Push notification support limited
- No background sync
- Reduced storage quota
- Hardware restrictions: No Bluetooth, NFC, advanced camera controls
- All browsers use WebKit (Safari dictates capabilities)
- Performance lower than native or Android PWA equivalents

## Tauri-Specific Capabilities Not Available in PWA

| Feature | Tauri | PWA |
|---------|-------|-----|
| **File System** | Full read/write anywhere | Sandboxed File System Access API (user-initiated) |
| **System Tray** | Full support with menus | Not available |
| **Native Menus** | OS-native application menus | Limited in-browser context menus only |
| **Background Processes** | True background services | Limited service workers only |
| **Custom Protocols** | System-wide protocol registration (`myapp://`) | Cannot register OS-level protocols |
| **Shell Access** | Execute system commands | Not available |
| **Window Management** | Full control, multi-window support | Browser window only |
| **Auto-Launch** | Can start on OS boot | Not available |
| **Binary Size** | ~3-10 MB (uses OS WebView) | N/A (network download) |
| **Offline Distribution** | Standalone executable | Requires initial network access |

## Storage Architecture Examples

### Tauri (Rust)

```rust
use loro::LoroDoc;
use rusqlite::Connection;
use std::path::PathBuf;
use std::fs;

pub struct Storage {
    loro_doc: LoroDoc,
    db: Connection,
    data_dir: PathBuf,
}

impl Storage {
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        // CRDT state persistence
        let loro_path = data_dir.join("loro_state.bin");
        let loro_doc = if loro_path.exists() {
            let bytes = fs::read(&loro_path)?;
            LoroDoc::from_bytes(&bytes)?
        } else {
            LoroDoc::new()
        };

        // SQL cache
        let db = Connection::open(data_dir.join("cache.db"))?;
        db.execute_batch(SCHEMA_SQL)?;

        Ok(Self { loro_doc, db, data_dir })
    }

    pub fn sync(&self) -> Result<()> {
        // Persist CRDT state to disk
        let snapshot = self.loro_doc.export_snapshot();
        fs::write(
            self.data_dir.join("loro_state.bin"),
            snapshot
        )?;
        Ok(())
    }

    pub fn query_cache(&self, sql: &str) -> Result<Vec<Row>> {
        // Standard SQLite queries
        let mut stmt = self.db.prepare(sql)?;
        // ... query execution
    }
}
```

### PWA (TypeScript with OPFS)

```typescript
import { Loro } from 'loro-crdt';
import initSqlite from 'wa-sqlite';

class BrowserStorage {
    private loro: Loro;
    private db: any;

    async init() {
        // Initialize CRDT from OPFS
        this.loro = new Loro();

        try {
            const root = await navigator.storage.getDirectory();
            const fileHandle = await root.getFileHandle('loro_state.bin', {
                create: true
            });
            const file = await fileHandle.getFile();

            if (file.size > 0) {
                const bytes = await file.arrayBuffer();
                this.loro.importUpdateBatch(new Uint8Array(bytes));
            }
        } catch (e) {
            console.error('OPFS not available, falling back to IndexedDB');
            // Fallback to IndexedDB
        }

        // Initialize SQLite
        const sqlite = await initSqlite();
        this.db = await sqlite.open('cache.db');
    }

    async sync() {
        // Persist CRDT to OPFS
        try {
            const snapshot = this.loro.exportSnapshot();
            const root = await navigator.storage.getDirectory();
            const fileHandle = await root.getFileHandle('loro_state.bin', {
                create: true
            });
            const writable = await fileHandle.createWritable();
            await writable.write(snapshot);
            await writable.close();
        } catch (e) {
            console.error('OPFS write failed');
            // Fallback to IndexedDB
        }
    }

    async queryCacheQuery(sql: string): Promise<any[]> {
        // wa-sqlite query execution
        // ... implementation
    }
}
```

## Technical Considerations for Offline-First Apps

### CRDT State Management

**Native (Tauri)**:
- Direct file I/O
- No size limitations (disk space only)
- Simple serialization to binary files
- File watching for external changes possible

**Browser (PWA)**:
- Storage quota limits (varies by browser, typically gigabytes but not guaranteed)
- Must request persistent storage permission
- Risk of eviction if storage pressure
- IndexedDB transactions for consistency
- OPFS for better performance when available

### SQL Query Performance

**Native SQLite (Tauri)**:
- Native performance
- Memory-mapped I/O
- Full SQLite feature set
- Large database support (multi-GB)

**Browser SQLite (PWA)**:
- WASM overhead (typically 1.5-3x slower than native)
- Storage backend affects performance (OPFS > IndexedDB)
- Same SQL syntax and features
- Practical limit around 1-2 GB depending on browser

### HTTP API Integration

Both platforms:
- Standard HTTP clients available
- CORS applies to browser (may need proxy)
- Service workers can intercept requests (PWA)
- Background requests possible in Tauri when app closed

### Third-Party API Caching Strategy

**Considerations for both platforms**:
- Store API responses in SQL for offline querying
- Use CRDT for local modifications to cached data
- Sync CRDT changes back to APIs when online
- Handle conflicts between local CRDT state and API state

**SQL schema example**:
```sql
CREATE TABLE todoist_tasks (
    id TEXT PRIMARY KEY,
    content TEXT,
    due_date TEXT,
    cached_at INTEGER,
    loro_version BLOB  -- CRDT version for this cached item
);
```

## Development and Deployment Characteristics

### Tauri

**Development**:
- Rust + frontend framework
- Need Rust toolchain installed
- Hot reload for frontend, rebuild for Rust changes
- Platform-specific testing (Windows, macOS, Linux)

**Deployment**:
- Build separate binaries per platform
- Code signing for macOS/Windows
- Auto-updater implementation needed for updates
- Distribution via GitHub releases, website download, or app stores

**Updates**:
- Users must download and install updates
- Or implement auto-update (Tauri has plugin)
- Can't push instant updates like web

### PWA

**Development**:
- TypeScript/JavaScript + framework
- Standard web development tooling
- Rust requires WASM compilation if used
- Test in multiple browsers

**Deployment**:
- Deploy to web hosting
- Single deployment for all platforms
- Service worker caching strategy needed
- Manifest.json configuration

**Updates**:
- Instant updates via service worker
- No user action required
- Can version APIs for migration

## Performance Characteristics

### Startup Time

**Tauri**:
- Native binary startup (~100-500ms)
- CRDT state loaded from disk
- SQLite database opened immediately

**PWA**:
- Network request for resources (first load)
- Service worker cache (subsequent loads)
- IndexedDB/OPFS open latency
- WASM initialization overhead

### Runtime Performance

**Tauri**:
- Native Rust performance for CRDT operations
- Native SQLite queries
- No browser sandbox overhead

**PWA**:
- WASM CRDT (typically 1.5-2x slower than native)
- WASM SQLite (1.5-3x slower than native)
- Browser security sandbox overhead
- JavaScript bridge costs

### Memory Usage

**Tauri**:
- Controlled memory usage
- Rust ownership model
- Native allocators

**PWA**:
- Browser memory management
- WASM linear memory limitations
- Garbage collection pauses

## Data Synchronization Patterns

### Local-First Sync (both platforms)

**Common pattern**:
1. Local CRDT contains source of truth
2. Changes made to local CRDT immediately
3. Background sync to third-party APIs
4. Merge remote changes into CRDT
5. Resolve conflicts automatically via CRDT

**Tauri advantages**:
- Can run background sync when app closed
- File system change watching
- System notifications when sync completes

**PWA characteristics**:
- Sync only when app open (or limited service worker)
- Background Sync API available (except iOS)
- Network-based notifications for sync status

### Conflict Resolution

**CRDT-based** (both platforms):
- Automatic conflict-free merging
- Deterministic results across devices
- No manual conflict resolution needed for CRDT data

**API synchronization conflicts**:
- Requires custom logic
- Last-write-wins, or
- CRDT version used as authoritative, or
- Manual user resolution

## Testing Considerations

### Tauri

**Test surfaces**:
- Rust backend unit tests
- Rust integration tests
- Frontend unit tests
- End-to-end tests per platform (Win/Mac/Linux)
- WebView compatibility tests

**Tools**:
- cargo test for Rust
- Frontend framework test tools
- WebDriver for E2E

### PWA

**Test surfaces**:
- JavaScript/TypeScript unit tests
- Service worker tests
- Browser compatibility tests (Chrome, Firefox, Safari, Edge)
- Mobile browser tests (Android, iOS)
- Offline functionality tests
- Storage quota handling tests

**Tools**:
- Jest, Vitest, etc.
- Playwright, Cypress for E2E
- Lighthouse for PWA compliance

## Summary: Key Differences

| Aspect | Tauri | PWA |
|--------|-------|-----|
| **Platforms** | Desktop (Win/Mac/Linux) | Desktop + Mobile (with iOS limitations) |
| **Distribution** | Download executable | Visit URL |
| **Storage** | Native file system, unlimited | Browser storage, quota-limited |
| **CRDT Performance** | Native Rust | WASM (1.5-2x slower) |
| **SQL Performance** | Native SQLite | WASM SQLite (1.5-3x slower) |
| **Background Sync** | Full support | Limited (none on iOS) |
| **Offline First** | Natural fit | Requires careful service worker setup |
| **Updates** | User action or auto-updater | Automatic |
| **OS Integration** | Deep (tray, menus, protocols) | Minimal |
| **Installation Friction** | Download + install | Zero (web) |
| **Code Reuse (if Rust)** | Native Rust | WASM compilation needed |
| **Binary Size** | 3-10 MB | N/A |
| **Development Complexity** | Moderate-High | Low-Moderate |

Neither approach is universally better - the choice depends on target platforms, required OS integration depth, performance requirements, and distribution preferences.
