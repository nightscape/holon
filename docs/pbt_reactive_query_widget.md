## Property-Based Test Plan: Reactive Query Widget Regression

### Context

The reproducible regression surfaced while editing Todoist tasks inside the Flutter frontend: typing into any `EditableTextField` pushes the change to Todoist, the CDC feed arrives with the new string, but the UI instantly reverts to the stale text that existed before the edit. Instrumentation shows the notifier cache updating correctly, so the bug must be downstream: whenever the main screen rebuilds (e.g., because `queryResultProvider` reexecutes), `ReactiveQueryWidget` is rebuilt with the original `initialData` snapshot, replacing the freshly updated cache. This document explains how to craft a property-based test that hits exactly that UI-level failure.

The existing property-based test (`frontends/flutter/test/render/reactive_query_notifier_pbt_test.dart`) only exercises the notifier in isolation, so it will always pass. To catch the real issue we have to include the app-specific wiring that causes the regression.

That notifier-only property test is still valuable: it proves that `ReactiveQueryStateNotifier` maintains consistent caches/orders for every possible CDC sequence, and it will keep protecting that invariant even after we fix the UI. The new widget-level test supplements it by covering the integration boundary that currently fails; neither test subsumes the other.

### Desired Property

> After any sequence of CDC updates, every rendered `EditableTextField` must still show the latest `content` value from the notifier **even if** the widget tree rebuilds (e.g., because a parent provider invalidated).

More formally:

1. Start with a non-empty initial dataset.
2. Apply a finite sequence of CDC `RowChange` events that mutate `content` fields.
3. Force the same rebuild pattern performed by the actual app (invalidate the parent provider or rebuild the widget subtree).
4. Inspect the text controllers used by `EditableTextField` widgets.

**Property**: For every row ID in `rowCache`, the rendered text equals the most recent `content` emitted for that row.

### Test Design

We need a widget-level harness, not just a provider container. The plan is to drive a minimal version of the real pipeline inside a `WidgetTester` while still using property-based input generation.

#### 1. Harness widget

Create a `ReactiveQueryHarness` widget in `frontends/flutter/test/helpers` that:

- Accepts a `List<Map<String, dynamic>> initialData` and a `StreamController<RowChange>`.
- Hosts a `ProviderScope` whose overrides mimic `main.dart`:
  - Provide `ReactiveQueryParams` with the supplied initial data and stream.
  - Use a `ConsumerWidget` that simply renders `ReactiveQueryWidget` with a deterministic PRQL spec (columns: `id`, `content`, etc.).
- Exposes the list of active `TextEditingController` instances via an injected callback or a `ValueNotifier<List<TextEditingController>>` so the test can read the rendered text without needing to scrape the widget tree manually.

This harness lets us recycle the real interpreter and widget-building logic while keeping the runtime deterministic.

#### 2. Property input model

Define a property-based generator that produces (using `kiri_check`’s stateful testing support, e.g., `Command`/`StatefulModel` builders):

- A non-empty initial dataset (list of rows with unique `id` + initial `content`).
- A sequence of operations, each being either:
  - `update(rowId, newContent)` – emits a `RowChange.updated`.
  - `rebuild` – simulates a provider invalidation/rebuild.

For `rebuild`, we can toggle a `Key` on the harness widget, or re-create the `ProviderScope` within the test to mirror what happens when `queryResultProvider` is invalidated.

#### 3. Running the property

For each generated test case:

1. Pump the harness with the generated initial data.
2. Drain the tester microtasks to allow the initial cache to settle.
3. Iterate through the generated operations:
   - For `update`, add the synthesized `RowChange.updated` to the stream controller and pump the tester.
   - For `rebuild`, re-pump the harness with a new `UniqueKey` (or rebuild the surrounding widget) without touching the stream/controller state.
4. After all operations, collect:
   - The notifier state (`container.read(reactiveQueryStateProvider(params))`).
   - The text currently displayed by every `EditableTextField` via the harness callback.

#### 4. Assertion

Compare each row’s expected `content` (from the notifier’s `rowCache`) with the corresponding text controller value. If any mismatch exists, the property fails. On the current buggy implementation the test will fail as soon as a `rebuild` appears after an `update`.

### Implementation Notes

- Run the property inside `testWidgets` so flutter_test can pump frames; wrap the property body with `propertyAsync` (from `kiri_check`) to allow asynchronous widget pumping.
- Use deterministic delays (`await tester.pump()` rather than `Future.delayed`) to keep the test fast and reliable.
- To avoid flaky state sharing between property runs, dispose the stream controller and the provider container at the end of each case.
- Keep logs enabled (`debugPrint`) so failures include the widget’s own instrumentation when shrinking finds a minimal counterexample.
- When modeling sequences, leverage `kiri_check`’s stateful facilities to describe operations as commands with pre/post conditions; this allows the shrinking engine to discover the minimal `(update → rebuild)` counterexample automatically.

### Expected Outcome

Once this property-based widget test is in place it will fail on the current codebase (because the UI reverts to old `initialData`). After we fix the root cause (preventing `ReactiveQueryWidget` from re-seeding with stale data), the property will pass and guard against regressions.
