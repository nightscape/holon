use super::flutter_pbt_backend::FlutterPbtBackend;
use flutter_rust_bridge::frb;

/// Flutter StateMachineTest implementation for property-based testing
///
/// This module implements the `StateMachineTest` trait for Flutter UI,
/// allowing proptest to generate random transitions and verify correctness
/// against the MemoryBackend reference implementation.
#[cfg(not(target_arch = "wasm32"))]
use holon::api::pbt_infrastructure::*;
#[cfg(not(target_arch = "wasm32"))]
use holon::api::repository::CoreOperations;
use holon::api::types::Traversal;
use std::collections::HashMap;

/// Flutter backend test state for property-based testing
#[frb(ignore)]
pub struct FlutterBlockTreeTest {
    pub backend: FlutterPbtBackend,
    /// ID mapping: MemoryBackend ID → FlutterPbtBackend ID
    pub id_map: HashMap<String, String>,
}

#[cfg(not(target_arch = "wasm32"))]
impl StateMachineTest for FlutterBlockTreeTest {
    type SystemUnderTest = Self;
    type Reference = ReferenceState;

    fn init_test(_ref_state: &ReferenceState) -> Self {
        // NOTE: This can't be implemented here because FlutterPbtBackend needs callbacks
        // which aren't available in this context. The manual runner will construct
        // FlutterBlockTreeTest directly with the callbacks.
        panic!("FlutterBlockTreeTest must be initialized manually with callbacks");
    }

    fn apply(mut state: Self, ref_state: &ReferenceState, transition: BlockTransition) -> Self {
        // Translate transition from MemoryBackend IDs to Flutter IDs
        let sut_transition = translate_transition(&transition, &state.id_map);
        eprintln!("[Flutter PBT] Translated transition: {:?}", sut_transition);

        // Apply the translated transition to Flutter backend
        let created_blocks = tokio::task::block_in_place(|| {
            ref_state
                .handle
                .block_on(apply_transition(&state.backend, &sut_transition))
        })
        .expect("Flutter transition should succeed");

        eprintln!("[Flutter PBT] Created {} blocks", created_blocks.len());

        // Update ID map for newly created blocks
        if !created_blocks.is_empty() {
            let ref_blocks = tokio::task::block_in_place(|| {
                ref_state
                    .handle
                    .block_on(ref_state.backend.get_all_blocks(Traversal::ALL_BUT_ROOT))
            })
            .expect("Failed to get reference blocks");

            update_id_map_after_create(
                &mut state.id_map,
                &transition,
                &ref_blocks,
                &created_blocks,
            );

            eprintln!(
                "[Flutter PBT] Updated ID map, now has {} entries",
                state.id_map.len()
            );
        }

        state
    }

    fn check_invariants(state: &Self, ref_state: &ReferenceState) {
        eprintln!("[Flutter PBT] Checking invariants...");

        // Verify structural equality between MemoryBackend and Flutter UI
        tokio::task::block_in_place(|| {
            verify_backends_match(&ref_state.backend, &state.backend, &ref_state.handle);
        });

        eprintln!("[Flutter PBT] ✅ Invariants passed!");
    }
}
