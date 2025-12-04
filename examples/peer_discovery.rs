use anyhow::Result;
use holon::sync::CollaborativeDoc;
use std::env;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Usage:");
        println!("  cargo run --example peer_discovery <doc_id> [peer_node_id]");
        println!();
        println!("Examples:");
        println!("  # First peer (will wait for others to connect):");
        println!("  cargo run --example peer_discovery project-notes");
        println!();
        println!("  # Second peer (connects to first peer):");
        println!("  cargo run --example peer_discovery project-notes <node_id_from_first_peer>");
        println!();
        println!("Both peers can send AND receive - this is true P2P!");
        println!("The <doc_id> must match on all peers to collaborate.");
        return Ok(());
    }

    let doc_id = &args[1];
    let peer_node_id = args.get(2);

    run_peer(doc_id, peer_node_id.map(|s| s.as_str())).await
}

async fn run_peer(doc_id: &str, peer_node_id: Option<&str>) -> Result<()> {
    println!("=== Starting P2P Peer ===");
    println!("📄 Document: '{}'", doc_id);
    println!();

    // Create a collaborative document with its own endpoint
    let doc = CollaborativeDoc::with_new_endpoint(doc_id.to_string()).await?;

    println!("✅ Peer ready!");
    println!("📋 My Node ID: {}", doc.node_id());
    println!();

    // Add some initial content
    let initial_text = format!("Hello from peer {}! ", &doc.node_id().to_string()[..8]);
    doc.insert_text("editor", 0, &initial_text).await?;
    println!("📝 Initial text: {}", doc.get_text("editor").await?);
    println!();

    match peer_node_id {
        Some(peer_id_str) => {
            // We have a peer to connect to - initiate connection
            println!("🔗 Connecting to peer: {}", peer_id_str);
            println!("   (I can still accept connections from others too!)");
            println!();

            let peer_public_key: iroh::PublicKey = peer_id_str.parse()?;
            let peer_addr = iroh::NodeAddr::new(peer_public_key);

            // Sync with the peer
            doc.connect_and_sync_to_peer(peer_addr).await?;

            println!("✅ Sync complete!");
            println!("📝 After sync: {}", doc.get_text("editor").await?);
        }
        None => {
            // No peer specified - just wait for incoming connections
            println!("⏳ Waiting for other peers to connect...");
            println!("   Share this command with other peers:");
            println!(
                "   cargo run --example peer_discovery {} {}",
                doc_id,
                doc.node_id()
            );
            println!();

            // Accept one connection (in real app, this would loop)
            doc.accept_sync_from_peer().await?;

            println!();
            println!("✅ Received sync from peer!");
            println!("📝 After sync: {}", doc.get_text("editor").await?);
        }
    }

    Ok(())
}
