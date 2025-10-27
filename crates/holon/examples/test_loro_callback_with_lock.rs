use loro::LoroDoc;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    println!("Testing Loro subscription callback that acquires read lock...\n");

    let doc = Arc::new(RwLock::new(LoroDoc::new()));
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    let doc_clone = doc.clone();

    // Register subscription with callback that spawns task to acquire read lock
    println!("Step 1: Registering subscription with lock-acquiring callback...");
    let _subscription = {
        let loro_doc = doc.write().await;

        let sub = loro_doc.subscribe_root(Arc::new(move |event| {
            println!("  → Callback invoked! Events: {}", event.events.len());

            let counter_inner = counter_clone.clone();
            let doc_inner = doc_clone.clone();

            // Spawn task to acquire read lock (mimics actual code)
            tokio::spawn(async move {
                println!("    → Spawned task trying to acquire read lock...");

                // Try to acquire read lock
                let loro_doc = doc_inner.read().await;
                println!("    ✓ Read lock acquired!");

                // Do something with the doc
                let _ = loro_doc.get_map("test");

                counter_inner.fetch_add(1, Ordering::SeqCst);
                println!("    ✓ Task complete, counter incremented");
                drop(loro_doc);
            });

            println!("  → Callback returning (task spawned)");
        }));

        let _ = loro_doc
            .export(loro::ExportMode::updates_owned(Default::default()))
            .unwrap();
        drop(loro_doc);
        println!("  Subscription registered, write lock released");
        sub
    };

    // Give spawned task time to run if needed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    println!(
        "  Counter after registration: {}\n",
        counter.load(Ordering::SeqCst)
    );

    // Perform operation
    println!("Step 2: Performing operation...");
    {
        let loro_doc = doc.write().await;
        println!("  Write lock acquired");
        println!("  Inserting...");
        loro_doc.get_map("test").insert("key1", "value1").unwrap();
        println!("  Committing...");
        loro_doc.commit();
        println!("  Commit returned");
        println!("  Exporting...");
        let _ = loro_doc
            .export(loro::ExportMode::updates_owned(Default::default()))
            .unwrap();
        println!("  Export returned");
        drop(loro_doc);
        println!("  Write lock released");
    }

    // Wait for spawned task to complete
    println!("  Waiting for spawned task...");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    println!("  Counter: {}\n", counter.load(Ordering::SeqCst));

    drop(_subscription);
    println!("Final counter: {}", counter.load(Ordering::SeqCst));
}
