use async_trait::async_trait;
use holon_api::Value;
use holon_core::traits::{CompletionStateInfo, MaybeSendSync, OperationResult, Result};
use std::collections::HashMap;

// ---- Test trait: affects attribute (single field, multiple fields, empty) ----

#[holon_macros::operations_trait]
#[async_trait]
pub trait AffectsTestOps<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    /// Single affects field
    #[holon_macros::affects("name")]
    async fn single_affects(&self, id: &str, name: String) -> Result<OperationResult>;

    /// Multiple affects fields
    #[holon_macros::affects("parent_id", "depth", "sort_key")]
    async fn multi_affects(&self, id: &str, parent_id: &str) -> Result<OperationResult>;

    /// No affects attribute
    async fn no_affects(&self, id: &str) -> Result<OperationResult>;
}

// ---- Test trait: triggered_by attribute ----

#[holon_macros::operations_trait]
#[async_trait]
pub trait TriggeredByTestOps<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    /// Identity mode: triggered_by provides the same parameter
    #[holon_macros::affects("title")]
    #[holon_macros::triggered_by(availability_of = "title")]
    async fn identity_trigger(&self, id: &str, title: &str) -> Result<OperationResult>;

    /// Transform mode: triggered_by maps to different parameters
    #[holon_macros::affects("parent_id", "sort_key")]
    #[holon_macros::triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_id"])]
    async fn transform_trigger(
        &self,
        id: &str,
        parent_id: &str,
        after_id: Option<&str>,
    ) -> Result<OperationResult>;

    /// Multiple triggered_by on same method
    #[holon_macros::affects("parent_id", "depth")]
    #[holon_macros::triggered_by(availability_of = "tree_position", providing = ["parent_id", "after_id"])]
    #[holon_macros::triggered_by(availability_of = "selected_id", providing = ["parent_id"])]
    async fn multi_trigger(
        &self,
        id: &str,
        parent_id: &str,
        after_id: Option<&str>,
    ) -> Result<OperationResult>;
}

// ---- Test trait: enum_from attribute ----

#[holon_macros::operations_trait]
#[async_trait]
pub trait EnumFromTestOps<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    fn get_states(&self) -> Vec<CompletionStateInfo>;

    #[holon_macros::affects("state")]
    #[holon_macros::enum_from(method = "get_states", param = "state")]
    async fn set_state(&self, id: &str, state: String) -> Result<OperationResult>;
}

// ---- Test trait: dispatch with various parameter types ----

#[holon_macros::operations_trait]
#[async_trait]
pub trait DispatchTestOps<T>: MaybeSendSync
where
    T: MaybeSendSync + 'static,
{
    async fn string_param(&self, id: &str, value: String) -> Result<OperationResult>;
    async fn bool_param(&self, id: &str, flag: bool) -> Result<OperationResult>;
    async fn i64_param(&self, id: &str, count: i64) -> Result<OperationResult>;
    async fn optional_param(&self, id: &str, maybe: Option<&str>) -> Result<OperationResult>;
    async fn value_param(&self, id: &str, data: Value) -> Result<OperationResult>;
    async fn hashmap_param(
        &self,
        fields: HashMap<String, Value>,
    ) -> Result<(String, OperationResult)>;
}

// ---- Mock implementations ----

struct MockOps;

#[async_trait]
impl DispatchTestOps<()> for MockOps {
    async fn string_param(&self, _id: &str, _value: String) -> Result<OperationResult> {
        Ok(OperationResult::irreversible(vec![]))
    }
    async fn bool_param(&self, _id: &str, _flag: bool) -> Result<OperationResult> {
        Ok(OperationResult::irreversible(vec![]))
    }
    async fn i64_param(&self, _id: &str, _count: i64) -> Result<OperationResult> {
        Ok(OperationResult::irreversible(vec![]))
    }
    async fn optional_param(&self, _id: &str, _maybe: Option<&str>) -> Result<OperationResult> {
        Ok(OperationResult::irreversible(vec![]))
    }
    async fn value_param(&self, _id: &str, _data: Value) -> Result<OperationResult> {
        Ok(OperationResult::irreversible(vec![]))
    }
    async fn hashmap_param(
        &self,
        _fields: HashMap<String, Value>,
    ) -> Result<(String, OperationResult)> {
        Ok(("new-id".to_string(), OperationResult::irreversible(vec![])))
    }
}

struct MockEnumOps;

#[async_trait]
impl EnumFromTestOps<()> for MockEnumOps {
    fn get_states(&self) -> Vec<CompletionStateInfo> {
        vec![
            CompletionStateInfo {
                state: "TODO".to_string(),
                progress: 0.0,
                is_done: false,
                is_active: false,
            },
            CompletionStateInfo {
                state: "DONE".to_string(),
                progress: 100.0,
                is_done: true,
                is_active: false,
            },
        ]
    }

    async fn set_state(&self, _id: &str, _state: String) -> Result<OperationResult> {
        Ok(OperationResult::irreversible(vec![]))
    }
}

// ======== #[affects] tests ========

#[test]
fn affects_single_field() {
    let ops = __operations_affects_test_ops::affects_test_ops("test", "t", "tests", "id");
    let op = ops.iter().find(|o| o.name == "single_affects").unwrap();
    assert_eq!(op.affected_fields, vec!["name"]);
}

#[test]
fn affects_multiple_fields() {
    let ops = __operations_affects_test_ops::affects_test_ops("test", "t", "tests", "id");
    let op = ops.iter().find(|o| o.name == "multi_affects").unwrap();
    assert_eq!(op.affected_fields, vec!["parent_id", "depth", "sort_key"]);
}

#[test]
fn affects_empty_when_absent() {
    let ops = __operations_affects_test_ops::affects_test_ops("test", "t", "tests", "id");
    let op = ops.iter().find(|o| o.name == "no_affects").unwrap();
    assert!(op.affected_fields.is_empty());
}

// ======== #[triggered_by] tests ========

#[test]
fn triggered_by_identity() {
    let ops = __operations_triggered_by_test_ops::triggered_by_test_ops("test", "t", "tests", "id");
    let op = ops.iter().find(|o| o.name == "identity_trigger").unwrap();
    assert_eq!(op.param_mappings.len(), 1);
    assert_eq!(op.param_mappings[0].from, "title");
    assert_eq!(op.param_mappings[0].provides, vec!["title"]);
}

#[test]
fn triggered_by_transform() {
    let ops = __operations_triggered_by_test_ops::triggered_by_test_ops("test", "t", "tests", "id");
    let op = ops.iter().find(|o| o.name == "transform_trigger").unwrap();
    assert_eq!(op.param_mappings.len(), 1);
    assert_eq!(op.param_mappings[0].from, "tree_position");
    assert_eq!(op.param_mappings[0].provides, vec!["parent_id", "after_id"]);
}

#[test]
fn triggered_by_multiple() {
    let ops = __operations_triggered_by_test_ops::triggered_by_test_ops("test", "t", "tests", "id");
    let op = ops.iter().find(|o| o.name == "multi_trigger").unwrap();
    assert_eq!(op.param_mappings.len(), 2);
    assert_eq!(op.param_mappings[0].from, "tree_position");
    assert_eq!(op.param_mappings[0].provides, vec!["parent_id", "after_id"]);
    assert_eq!(op.param_mappings[1].from, "selected_id");
    assert_eq!(op.param_mappings[1].provides, vec!["parent_id"]);
}

// ======== #[enum_from] tests ========

#[test]
fn enum_from_resolver() {
    let mock = MockEnumOps;
    let ops = __operations_enum_from_test_ops::enum_from_test_ops_with_resolver(
        &mock, "test", "t", "tests", "id",
    );
    let op = ops.iter().find(|o| o.name == "set_state").unwrap();
    let param = op
        .required_params
        .iter()
        .find(|p| p.name == "state")
        .unwrap();
    match &param.type_hint {
        holon_api::TypeHint::OneOf { values } => {
            assert_eq!(values.len(), 2);
        }
        other => panic!("Expected TypeHint::OneOf, got {:?}", other),
    }
}

// ======== Dispatch metadata tests ========

#[test]
fn dispatch_operation_metadata() {
    let ops = __operations_dispatch_test_ops::dispatch_test_ops("test", "t", "tests", "id");
    let names: Vec<&str> = ops.iter().map(|o| o.name.as_str()).collect();
    assert!(names.contains(&"string_param"));
    assert!(names.contains(&"bool_param"));
    assert!(names.contains(&"i64_param"));
    assert!(names.contains(&"optional_param"));
    assert!(names.contains(&"value_param"));
    assert!(names.contains(&"hashmap_param"));
}

#[test]
fn dispatch_params_have_correct_types() {
    let ops = __operations_dispatch_test_ops::dispatch_test_ops("test", "t", "tests", "id");

    let op = ops.iter().find(|o| o.name == "bool_param").unwrap();
    let flag_param = op
        .required_params
        .iter()
        .find(|p| p.name == "flag")
        .unwrap();
    assert_eq!(flag_param.type_hint, holon_api::TypeHint::Bool);

    let op = ops.iter().find(|o| o.name == "i64_param").unwrap();
    let count_param = op
        .required_params
        .iter()
        .find(|p| p.name == "count")
        .unwrap();
    assert_eq!(count_param.type_hint, holon_api::TypeHint::Number);
}

#[test]
fn operation_descriptor_has_description_from_doc_comment() {
    let ops = __operations_affects_test_ops::affects_test_ops("test", "t", "tests", "id");
    let op = ops.iter().find(|o| o.name == "single_affects").unwrap();
    assert!(
        op.description.contains("Single affects field"),
        "Description should come from doc comment: '{}'",
        op.description
    );
}

// ======== Dispatch runtime tests ========

#[tokio::test]
async fn dispatch_string_param() {
    let mock = MockOps;
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::String("test-1".to_string()));
    params.insert("value".to_string(), Value::String("hello".to_string()));
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "string_param", &params).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_bool_param() {
    let mock = MockOps;
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::String("test-1".to_string()));
    params.insert("flag".to_string(), Value::Boolean(true));
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "bool_param", &params).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_i64_param() {
    let mock = MockOps;
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::String("test-1".to_string()));
    params.insert("count".to_string(), Value::Integer(42));
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "i64_param", &params).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_optional_param_present() {
    let mock = MockOps;
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::String("test-1".to_string()));
    params.insert("maybe".to_string(), Value::String("here".to_string()));
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "optional_param", &params).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_optional_param_absent() {
    let mock = MockOps;
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::String("test-1".to_string()));
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "optional_param", &params).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_missing_required_param_errors() {
    let mock = MockOps;
    let params = HashMap::new();
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "string_param", &params).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("id"),
        "Error should mention missing 'id' param: {err_msg}"
    );
}

#[tokio::test]
async fn dispatch_unknown_operation_errors() {
    let mock = MockOps;
    let params = HashMap::new();
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "nonexistent", &params).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn dispatch_hashmap_param() {
    let mock = MockOps;
    let mut params = HashMap::new();
    params.insert("name".to_string(), Value::String("test".to_string()));
    let result =
        __operations_dispatch_test_ops::dispatch_operation(&mock, "hashmap_param", &params).await;
    assert!(result.is_ok());
}
