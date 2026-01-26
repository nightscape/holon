//! TUI-R3BL StateMachineTest implementation for property-based testing
//!
//! This module implements the `StateMachineTest` trait for tui,
//! allowing proptest to generate random transitions and verify correctness
//! against the MemoryBackend reference implementation.

use super::tui_pbt_backend::TuiR3blPbtBackend;
use ferrous_di::{Resolver, ServiceCollection, ServiceCollectionModuleExt};
use holon::api::backend_engine::BackendEngine;
use holon::api::operation_dispatcher::OperationModule;
use holon::api::pbt_infrastructure::*;
use holon::api::repository::CoreOperations;
use holon::api::types::Traversal;
use holon::di;
use std::collections::HashMap;
use std::sync::Arc;

/// TUI-R3BL backend test state for property-based testing
pub struct TuiR3blBlockTreeTest {
    pub backend: TuiR3blPbtBackend,
    /// ID mapping: MemoryBackend ID → TuiR3blPbtBackend ID
    pub id_map: HashMap<String, String>,
    /// Runtime for async operations
    pub runtime: Arc<tokio::runtime::Runtime>,
}

impl StateMachineTest for TuiR3blBlockTreeTest {
    type SystemUnderTest = Self;
    type Reference = ReferenceState;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());

        // Create in-memory BackendEngine using dependency injection
        let engine = runtime.block_on(async {
            // Set up dependency injection container
            let mut services = ServiceCollection::new();

            // Register OperationModule to collect providers from DI
            services
                .add_module_mut(OperationModule)
                .expect("Failed to register OperationModule");

            // Register core services with in-memory database
            di::register_core_services(&mut services, ":memory:".into())
                .expect("Failed to register core services");

            // Build the DI container and resolve BackendEngine
            let provider = services.build();
            Resolver::get_required::<BackendEngine>(&provider)
        });

        // Initialize blocks table schema
        let backend_wrapper = TuiR3blPbtBackend::new(engine);
        runtime
            .block_on(backend_wrapper.ensure_schema())
            .expect("Failed to initialize schema");

        // Populate id_map with initial blocks (root + first child)
        let mut id_map = HashMap::new();
        runtime
            .block_on(populate_initial_id_map(
                &mut id_map,
                &ref_state.backend,
                &backend_wrapper,
            ))
            .expect("Failed to populate initial ID map");

        TuiR3blBlockTreeTest {
            backend: backend_wrapper,
            id_map,
            runtime,
        }
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        // Translate the transition from MemoryBackend IDs → TuiR3blPbtBackend IDs
        let sut_transition = translate_transition(&transition, &state.id_map);

        // Apply the translated transition to tui backend
        let created_blocks = state
            .runtime
            .block_on(apply_transition(&state.backend, &sut_transition))
            .expect("TuiR3bl transition should succeed");

        // Update ID map for newly created blocks
        if !created_blocks.is_empty() {
            let ref_blocks = state
                .runtime
                .block_on(ref_state.backend.get_all_blocks(Traversal::ALL_BUT_ROOT))
                .expect("Failed to get reference blocks");

            update_id_map_after_create(
                &mut state.id_map,
                &transition,
                &ref_blocks,
                &created_blocks,
            );
        }

        state
    }

    fn check_invariants(state: &Self, ref_state: &ReferenceState) {
        // Verify structural equality between MemoryBackend and tui backend
        verify_backends_match(&ref_state.backend, &state.backend, &ref_state.handle);
    }
}
