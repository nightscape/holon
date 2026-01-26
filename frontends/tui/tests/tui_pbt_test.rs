//! Property-based tests for tui frontend
//!
//! This module runs property-based tests against the tui backend
//! to verify it behaves identically to the MemoryBackend reference implementation.

use proptest::prelude::*;
use tui_r3bl_frontend::tui_pbt_state_machine::TuiR3blBlockTreeTest;

proptest_state_machine::prop_state_machine! {
    #![proptest_config(ProptestConfig {
        cases: 5,
        failure_persistence: None,
        timeout: 3000,
        verbose: 2,
        .. ProptestConfig::default()
    })]

    #[test]
    fn test_tui_r3bl_backend_state_machine(sequential 1..20 => TuiR3blBlockTreeTest);
}
