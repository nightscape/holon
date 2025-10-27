use loro::LoroDoc;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    println!("Testing Loro subscription across multiple lock acquisitions...\n");

    // Mimic the actual code structure:
    // 1. Register subscription in one with_write() call
    // 2. Perform operations in separate with_write() calls

    let doc = Arc::new(RwLock::new(LoroDoc::new()));
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    // Step 1: Register subscription (like watch_changes_since())
    println!("Step 1: Registering subscription...");
    let _subscription = {
        let loro_doc = doc.write().await;
        let sub = loro_doc.subscribe_root(Arc::new(move |event| {
            println!("  ✓ Callback fired! Events: {}", event.events.len());
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));

        // Mimic with_write's export() call
        let _ = loro_doc
            .export(loro::ExportMode::updates_owned(Default::default()))
            .unwrap();
        drop(loro_doc);
        println!("  Subscription registered, lock released");
        sub
    };

    println!(
        "  Counter after registration: {}\n",
        counter.load(Ordering::SeqCst)
    );

    // Step 2: Perform first operation (like create_block())
    println!("Step 2: First operation...");
    {
        let loro_doc = doc.write().await;
        println!("  Inserting key1...");
        loro_doc.get_map("test").insert("key1", "value1").unwrap();
        println!("  Committing...");
        loro_doc.commit();
        println!("  Exporting...");
        let _ = loro_doc
            .export(loro::ExportMode::updates_owned(Default::default()))
            .unwrap();
        drop(loro_doc);
        println!("  Lock released");
    }
    println!(
        "  Counter after first operation: {}\n",
        counter.load(Ordering::SeqCst)
    );

    // Step 3: Perform second operation
    println!("Step 3: Second operation...");
    {
        let loro_doc = doc.write().await;
        println!("  Inserting key2...");
        loro_doc.get_map("test").insert("key2", "value2").unwrap();
        println!("  Committing...");
        loro_doc.commit();
        println!("  Exporting...");
        let _ = loro_doc
            .export(loro::ExportMode::updates_owned(Default::default()))
            .unwrap();
        drop(loro_doc);
        println!("  Lock released");
    }
    println!(
        "  Counter after second operation: {}\n",
        counter.load(Ordering::SeqCst)
    );

    // Keep subscription alive
    drop(_subscription);

    println!("Final counter: {}", counter.load(Ordering::SeqCst));
    println!("\nTest complete!");
}
