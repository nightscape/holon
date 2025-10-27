//! Bidirectional sync integration tests for OrgMode ↔ Backend
//!
//! Tests the full sync loop:
//! - Forward: External org file edit → OrgAdapter → Backend (Loro/Turso)
//! - Backward: UI mutation via BackendEngine → OrgFileWriter → Org file on disk
//! - Round-trip: Both directions in sequence, verifying stability

use std::sync::Arc;
use std::time::Duration;

use holon_integration_tests::TestEnvironmentBuilder;

fn runtime() -> Arc<tokio::runtime::Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create runtime"),
    )
}

const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

// =============================================================================
// Forward Sync: External Org File → Backend
// =============================================================================

#[test]
fn forward_sync_pre_existing_file_syncs_on_startup() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Hello World\n:PROPERTIES:\n:ID: block-1\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        assert!(
            env.wait_for_block("block-1", SYNC_TIMEOUT).await,
            "Pre-existing block should sync on startup"
        );

        let rows = env
            .query("from blocks | filter id == \"block-1\" | select {id, content} | render (list item_template:(row (text this.content)))")
            .await
            .expect("query failed");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("content").and_then(|v| v.as_string()),
            Some("Hello World")
        );
    });
}

#[test]
fn forward_sync_external_edit_adds_block() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file("test.org", "* First\n:PROPERTIES:\n:ID: block-1\n:END:\n")
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // Externally add a second block
        let new_content = "\
* First
:PROPERTIES:
:ID: block-1
:END:
* Second
:PROPERTIES:
:ID: block-2
:END:
";
        let file_path = env.org_file_path("test.org");
        tokio::fs::write(&file_path, new_content)
            .await
            .expect("write failed");

        assert!(
            env.wait_for_block("block-2", SYNC_TIMEOUT).await,
            "Externally added block should appear in backend"
        );
    });
}

#[test]
fn forward_sync_external_edit_updates_content() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Original\n:PROPERTIES:\n:ID: block-1\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // Externally modify block content
        let new_content = "* Updated Content\n:PROPERTIES:\n:ID: block-1\n:END:\n";
        let file_path = env.org_file_path("test.org");
        tokio::fs::write(&file_path, new_content)
            .await
            .expect("write failed");

        // Wait for the update to propagate
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let rows = env
                .query("from blocks | filter id == \"block-1\" | select {id, content} | render (list item_template:(row (text this.content)))")
                .await
                .expect("query failed");
            if let Some(row) = rows.first() {
                if row.get("content").and_then(|v| v.as_string()) == Some("Updated Content") {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Externally updated content did not propagate to backend within timeout");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

#[test]
fn forward_sync_external_delete_removes_block() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* First\n:PROPERTIES:\n:ID: block-1\n:END:\n* Second\n:PROPERTIES:\n:ID: block-2\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;
        env.wait_for_block("block-2", SYNC_TIMEOUT).await;

        // Externally remove block-2
        let new_content = "* First\n:PROPERTIES:\n:ID: block-1\n:END:\n";
        let file_path = env.org_file_path("test.org");
        tokio::fs::write(&file_path, new_content)
            .await
            .expect("write failed");

        // Wait for deletion to propagate
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let rows = env
                .query("from blocks | select {id} | render (list item_template:(row (text this.id)))")
                .await
                .expect("query failed");
            let ids: Vec<&str> = rows
                .iter()
                .filter_map(|r| r.get("id").and_then(|v| v.as_string()))
                .collect();
            if ids.contains(&"block-1") && !ids.contains(&"block-2") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "Externally deleted block-2 still present in backend. IDs: {:?}",
                    ids
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

#[test]
fn forward_sync_source_block() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Heading\n:PROPERTIES:\n:ID: block-1\n:END:\n#+begin_src python\nprint('hello')\n#+end_src\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // Wait for the source block to appear
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let rows = env
                .query("from blocks | filter content_type == \"source\" | select {id, content, source_language} | render (list item_template:(row (text this.content)))")
                .await
                .expect("query failed");
            if let Some(row) = rows.first() {
                assert_eq!(
                    row.get("source_language").and_then(|v| v.as_string()),
                    Some("python")
                );
                let content = row.get("content").and_then(|v| v.as_string()).unwrap_or("");
                assert_eq!(content.trim(), "print('hello')");
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Source block did not appear in backend");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

// =============================================================================
// Backward Sync: UI Mutation → Org File
// =============================================================================

#[test]
fn backward_sync_ui_create_writes_to_org_file() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Existing\n:PROPERTIES:\n:ID: block-1\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // Get document URI for block-1's parent
        let rows = env
            .query("from blocks | filter id == \"block-1\" | select {parent_id} | render (list item_template:(row (text this.parent_id)))")
            .await
            .expect("query failed");
        let parent_id = rows[0]
            .get("parent_id")
            .and_then(|v| v.as_string())
            .expect("block-1 should have parent_id");

        // UI creates a new block under the same document
        env.create_block("block-new", parent_id, "New Block")
            .await
            .expect("create_block failed");

        // Wait for org file to contain the new block
        let file_path = env.org_file_path("test.org");
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .expect("read failed");
            if content.contains("New Block") && content.contains("block-new") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let content = tokio::fs::read_to_string(&file_path)
                    .await
                    .unwrap_or_default();
                panic!(
                    "UI-created block did not appear in org file within timeout.\nOrg content:\n{}",
                    content
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

#[test]
fn backward_sync_ui_update_writes_to_org_file() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Original Title\n:PROPERTIES:\n:ID: block-1\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // UI updates block content
        env.update_block_content("block-1", "Modified Title")
            .await
            .expect("update failed");

        // Wait for org file to reflect the update
        let file_path = env.org_file_path("test.org");
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .expect("read failed");
            if content.contains("Modified Title") && !content.contains("Original Title") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let content = tokio::fs::read_to_string(&file_path)
                    .await
                    .unwrap_or_default();
                panic!(
                    "UI update did not propagate to org file within timeout.\nOrg content:\n{}",
                    content
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

#[test]
fn backward_sync_ui_delete_removes_from_org_file() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Keep\n:PROPERTIES:\n:ID: block-1\n:END:\n* Delete Me\n:PROPERTIES:\n:ID: block-2\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;
        env.wait_for_block("block-2", SYNC_TIMEOUT).await;

        // UI deletes block-2
        env.delete_block("block-2")
            .await
            .expect("delete failed");

        // Wait for org file to no longer contain block-2
        let file_path = env.org_file_path("test.org");
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .expect("read failed");
            if content.contains("Keep") && !content.contains("Delete Me") && !content.contains("block-2") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let content = tokio::fs::read_to_string(&file_path)
                    .await
                    .unwrap_or_default();
                panic!(
                    "UI delete did not propagate to org file within timeout.\nOrg content:\n{}",
                    content
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

// =============================================================================
// Round-Trip: Forward + Backward in Sequence
// =============================================================================

#[test]
fn roundtrip_external_add_then_ui_update_then_verify_org() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file("test.org", "* Start\n:PROPERTIES:\n:ID: block-1\n:END:\n")
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // Step 1: External add of block-2
        let new_content = "\
* Start
:PROPERTIES:
:ID: block-1
:END:
* Added Externally
:PROPERTIES:
:ID: block-2
:END:
";
        let file_path = env.org_file_path("test.org");
        tokio::fs::write(&file_path, new_content)
            .await
            .expect("write failed");

        assert!(
            env.wait_for_block("block-2", SYNC_TIMEOUT).await,
            "External block-2 should sync"
        );

        // Step 2: Wait for write window so UI mutation proceeds cleanly
        env.wait_for_write_window_expiry().await;

        // Step 3: UI updates block-2 content
        env.update_block_content("block-2", "Modified By UI")
            .await
            .expect("update failed");

        // Step 4: Verify org file reflects the UI update
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .expect("read failed");
            if content.contains("Modified By UI") {
                // Also verify block-1 is still there
                assert!(
                    content.contains("block-1"),
                    "block-1 should still be in org file"
                );
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let content = tokio::fs::read_to_string(&file_path)
                    .await
                    .unwrap_or_default();
                panic!(
                    "Round-trip failed: UI update not in org file.\nOrg content:\n{}",
                    content
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

#[test]
fn roundtrip_ui_create_then_external_update_then_verify_backend() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Root\n:PROPERTIES:\n:ID: block-1\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // Step 1: UI creates block-2
        let rows = env
            .query("from blocks | filter id == \"block-1\" | select {parent_id} | render (list item_template:(row (text this.parent_id)))")
            .await
            .expect("query failed");
        let doc_parent = rows[0]
            .get("parent_id")
            .and_then(|v| v.as_string())
            .expect("block-1 should have parent_id");

        env.create_block("block-2", doc_parent, "UI Created")
            .await
            .expect("create_block failed");

        // Wait for org file to contain block-2
        let file_path = env.org_file_path("test.org");
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .expect("read failed");
            if content.contains("block-2") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("UI-created block-2 not in org file");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Step 2: Wait for write window to expire
        env.wait_for_write_window_expiry().await;

        // Step 3: External edit changes block-2 content
        let content = tokio::fs::read_to_string(&file_path)
            .await
            .expect("read failed");
        let updated = content.replace("UI Created", "Externally Updated");
        tokio::fs::write(&file_path, &updated)
            .await
            .expect("write failed");

        // Step 4: Verify backend reflects the external update
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let rows = env
                .query("from blocks | filter id == \"block-2\" | select {content} | render (list item_template:(row (text this.content)))")
                .await
                .expect("query failed");
            if let Some(row) = rows.first() {
                if row.get("content").and_then(|v| v.as_string()) == Some("Externally Updated") {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("External update to block-2 did not propagate to backend");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

// =============================================================================
// Stability: No Sync Loops
// =============================================================================

#[test]
fn stability_no_duplicate_blocks_after_ui_mutation() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Block A\n:PROPERTIES:\n:ID: block-1\n:END:\n* Block B\n:PROPERTIES:\n:ID: block-2\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;
        env.wait_for_block("block-2", SYNC_TIMEOUT).await;

        // UI update — this triggers OrgFileWriter → org file write → file watcher
        // WriteTracker must prevent OrgAdapter from re-reading the file
        env.update_block_content("block-1", "Updated A")
            .await
            .expect("update failed");

        // Wait for the org file to be updated
        let file_path = env.org_file_path("test.org");
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .expect("read failed");
            if content.contains("Updated A") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Org file never updated");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wait extra to ensure no re-processing loop creates duplicates
        tokio::time::sleep(Duration::from_millis(3000)).await;

        let rows = env
            .query("from blocks | select {id} | render (list item_template:(row (text this.id)))")
            .await
            .expect("query failed");
        let ids: Vec<&str> = rows
            .iter()
            .filter_map(|r| r.get("id").and_then(|v| v.as_string()))
            .collect();

        assert_eq!(ids.len(), 2, "Expected exactly 2 blocks, got: {:?}", ids);
        assert!(ids.contains(&"block-1"));
        assert!(ids.contains(&"block-2"));
    });
}

#[test]
fn stability_multiple_rapid_ui_updates_converge() {
    let rt = runtime();
    rt.block_on(async {
        let env = TestEnvironmentBuilder::new()
            .with_org_file(
                "test.org",
                "* Counter\n:PROPERTIES:\n:ID: block-1\n:END:\n",
            )
            .build(rt.clone())
            .await
            .expect("Failed to build environment");

        env.wait_for_block("block-1", SYNC_TIMEOUT).await;

        // Rapid-fire 5 UI updates
        for i in 1..=5 {
            env.update_block_content("block-1", &format!("Update {}", i))
                .await
                .expect("update failed");
        }

        // Wait for final state to propagate to org file
        let file_path = env.org_file_path("test.org");
        let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
        loop {
            let content = tokio::fs::read_to_string(&file_path)
                .await
                .expect("read failed");
            if content.contains("Update 5") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let content = tokio::fs::read_to_string(&file_path)
                    .await
                    .unwrap_or_default();
                panic!(
                    "Rapid updates did not converge to final state.\nOrg content:\n{}",
                    content
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Verify backend also has the final state
        let rows = env
            .query("from blocks | filter id == \"block-1\" | select {content} | render (list item_template:(row (text this.content)))")
            .await
            .expect("query failed");
        assert_eq!(
            rows[0].get("content").and_then(|v| v.as_string()),
            Some("Update 5")
        );

        // Verify no duplicates
        let all = env
            .query("from blocks | select {id} | render (list item_template:(row (text this.id)))")
            .await
            .expect("query failed");
        assert_eq!(all.len(), 1, "Expected 1 block, got: {}", all.len());
    });
}
