# Spec 0002: Fix indent/outdent operation dispatch

## Problem Statement
- Todoist block operations such as `indent` and `outdent` surface `Unknown operation ... for trait MutableTaskDataSource` errors even though those operations are defined on `MutableBlockDataSource`.
- The current dispatcher falls through to the next trait whenever a dispatch attempt returns *any* error, so legitimate execution errors from `MutableBlockDataSource` are swallowed and misreported as "unknown operation" coming from `MutableTaskDataSource`.
- Because the real error is hidden, the IDE loop cannot diagnose why indent/outdent failed, and downstream systems believe the datasource does not support block operations.

## Goals / Requirements
1. Distinguish "operation not handled" errors from real execution failures so that only unknown operations fall through to the next trait.
2. Ensure all generated dispatchers report unknown operations using a structured error type that can be detected by callers.
3. Update `TodoistTaskDataSource` and its wrappers to stop on the first non-unknown error and propagate it to callers.
4. Keep backward compatibility for existing providers that already rely on the generated dispatch helpers.

## Non-Goals
- Implementing the actual indent/outdent business logic or parent/sort-key updates (handled separately).
- Changing the public `OperationDescriptor` schema or adding new operations.

## Proposed Solution
1. Introduce a new `UnknownOperationError` type (and helper) in `core::datasource` that encapsulates the "unknown operation" condition.
2. Update the `#[operations_trait]` macro so every generated dispatcher returns `UnknownOperationError` when an operation name misses all branches.
3. Teach `TodoistTaskDataSource::execute_operation`, `TodoistOperationProvider`, and `TodoistFakeOperationProvider` to:
   - Attempt each trait dispatcher in order.
   - Return immediately on success.
   - On error, inspect it via the helper; if it is not `UnknownOperationError`, propagate it immediately.
   - Only continue to the next trait when the error *is* `UnknownOperationError`.

## Acceptance Criteria
- Issuing an `indent`/`outdent` operation no longer falls through to `MutableTaskDataSource` unless the operation is truly unsupported.
- The backend now surfaces the *actual* execution error when block operations fail (e.g., parameter extraction, set_field not supported), enabling further fixes.
- Regression: other providers compiling the macro (e.g., fake/provider wrappers) continue to build without changes beyond the new helper import.
