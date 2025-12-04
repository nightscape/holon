# Plan 0002: Fix indent/outdent operation dispatch

Derived from [Spec 0002](../specs/0002-indent-outdent-dispatch.md).

## Phase 1 – Add structured unknown-operation error
- Define `UnknownOperationError` (plus helper) in `core::datasource` so callers can detect the "operation not handled" case without string matching.
- Update the `#[operations_trait]` macro to emit that error whenever `dispatch_operation` hits the default arm.
- Rationale: provides an explicit signal that downstream dispatchers can act on.

## Phase 2 – Fix dispatcher fall-through logic
- Update `TodoistTaskDataSource::execute_operation` and both todoist wrappers (`provider_wrapper`, `fake_wrapper`) to:
  - Attempt each trait dispatcher in order.
  - Stop on success, and stop on the first error that is *not* `UnknownOperationError`.
  - Only continue when the error is `UnknownOperationError`.
- Rationale: prevents real block-operation failures from being misreported as "unknown operation".

## Phase 3 – Validation & follow-up
- Rebuild the workspace (incremental `cargo check`) to ensure macro + datasource changes compile.
- Manually reason about the new error propagation path (outdent should now surface its true error).
- Document residual risks (e.g., actual indent/outdent semantics still depend on follow-up work) in the review.
