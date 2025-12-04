use loro::LoroDoc;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

fn main() {
    println!("Testing Loro subscription basics...\n");

    // Test 1: Basic subscription without locks
    {
        println!("Test 1: Basic subscription (no locks)");
        let doc = LoroDoc::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let _sub = doc.subscribe_root(Arc::new(move |event| {
            println!("  ✓ Callback fired! Events: {}", event.events.len());
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));

        doc.get_map("test").insert("key", "value").unwrap();
        doc.commit();

        println!(
            "  Counter after commit: {}\n",
            counter.load(Ordering::SeqCst)
        );
    }

    // Test 2: Subscription with Arc<RwLock<>> (mimics actual code)
    {
        println!("Test 2: Subscription with Arc<RwLock<>>");
        use tokio::sync::RwLock;

        let doc = Arc::new(RwLock::new(LoroDoc::new()));
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        // Register subscription
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let loro_doc = doc.write().await;
            let _sub = loro_doc.subscribe_root(Arc::new(move |event| {
                println!("  ✓ Callback fired! Events: {}", event.events.len());
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }));

            // Perform operation
            loro_doc.get_map("test").insert("key", "value").unwrap();
            loro_doc.commit();

            println!("  Counter after commit: {}", counter.load(Ordering::SeqCst));
            drop(loro_doc); // explicitly drop lock
        });

        println!(
            "  Counter after lock released: {}\n",
            counter.load(Ordering::SeqCst)
        );
    }

    // Test 3: Callback triggered by export() vs commit()
    {
        println!("Test 3: Export triggering vs Commit triggering");
        let doc = LoroDoc::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let _sub = doc.subscribe_root(Arc::new(move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            println!(
                "  ✓ Callback triggered (count: {})",
                counter_clone.load(Ordering::SeqCst)
            );
        }));

        doc.get_map("test").insert("key1", "value1").unwrap();
        println!(
            "  After insert, before commit: {}",
            counter.load(Ordering::SeqCst)
        );

        doc.commit();
        println!("  After commit: {}", counter.load(Ordering::SeqCst));

        doc.export(loro::ExportMode::updates_owned(Default::default()))
            .unwrap();
        println!("  After export: {}", counter.load(Ordering::SeqCst));

        // Make another change
        doc.get_map("test").insert("key2", "value2").unwrap();
        doc.commit();
        println!(
            "  After second commit: {}\n",
            counter.load(Ordering::SeqCst)
        );
    }

    println!("All tests complete!");
}
