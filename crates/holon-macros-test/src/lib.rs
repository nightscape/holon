// Test crate for holon-macros
// This allows us to test the macro expansion since proc macros can't be used in their own crate

use async_trait::async_trait;
use holon::core::datasource::{Result, UndoAction};
use holon_macros::require;

// Test trait with require attributes
#[holon_macros::operations_trait]
#[async_trait]
pub trait TestTrait<T>: Send + Sync
where
    T: Send + Sync + 'static,
{
    /// Delete an item by ID
    #[require(id.len() > 0)]
    async fn delete(&self, id: &str) -> Result<UndoAction>;

    /// Set a boolean flag
    #[require(value == true || value == false)]
    async fn set_flag(&self, _id: &str, value: bool) -> Result<UndoAction>;

    /// Set priority with range check
    #[require(priority >= 1)]
    #[require(priority <= 5)]
    async fn set_priority(&self, _id: &str, priority: i64) -> Result<UndoAction>;

    /// Method without precondition
    async fn no_precondition(&self, id: &str) -> Result<UndoAction>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use holon_api::Value;
    use std::any::Any;
    use std::collections::HashMap;

    #[test]
    fn test_require_precondition_extraction() {
        // Test that preconditions are extracted and included in OperationDescriptor
        let ops = __operations_test_trait::test_trait("test-entity", "test_table", "id");

        // Find delete operation
        let delete_op = ops.iter().find(|op| op.name == "delete").unwrap();
        assert!(
            delete_op.precondition.is_some(),
            "delete operation should have precondition"
        );

        // Find set_flag operation
        let set_flag_op = ops.iter().find(|op| op.name == "set_flag").unwrap();
        assert!(
            set_flag_op.precondition.is_some(),
            "set_flag operation should have precondition"
        );

        // Find set_priority operation (should have combined preconditions)
        let set_priority_op = ops.iter().find(|op| op.name == "set_priority").unwrap();
        assert!(
            set_priority_op.precondition.is_some(),
            "set_priority operation should have precondition"
        );

        // Find no_precondition operation
        let no_precondition_op = ops.iter().find(|op| op.name == "no_precondition").unwrap();
        assert!(
            no_precondition_op.precondition.is_none(),
            "no_precondition operation should not have precondition"
        );
    }

    #[test]
    fn test_precondition_closure_evaluation() {
        let ops = __operations_test_trait::test_trait("test-entity", "test_table", "id");

        // Test delete operation precondition
        let delete_op = ops.iter().find(|op| op.name == "delete").unwrap();
        let precondition = delete_op.precondition.as_ref().unwrap();

        // Create test parameters - wrap Value in Box<dyn Any>
        let mut params_valid: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_valid.insert(
            "id".to_string(),
            Box::new(Value::String("test123".to_string())),
        );

        let mut params_invalid: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_invalid.insert("id".to_string(), Box::new(Value::String("".to_string())));

        // Test valid precondition
        let result_valid = precondition(&params_valid);
        assert!(
            result_valid.is_ok(),
            "Precondition should pass for valid input"
        );
        assert_eq!(
            result_valid.unwrap(),
            true,
            "Precondition should return true"
        );

        // Test invalid precondition
        let result_invalid = precondition(&params_invalid);
        assert!(result_invalid.is_ok(), "Precondition should not error");
        assert_eq!(
            result_invalid.unwrap(),
            false,
            "Precondition should return false for empty string"
        );
    }

    #[test]
    fn test_precondition_with_multiple_requires() {
        let ops = __operations_test_trait::test_trait("test-entity", "test_table", "id");

        // Test set_priority operation which has multiple require attributes
        let set_priority_op = ops.iter().find(|op| op.name == "set_priority").unwrap();
        let precondition = set_priority_op.precondition.as_ref().unwrap();

        // Test valid priority (within range)
        let mut params_valid: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_valid.insert(
            "id".to_string(),
            Box::new(Value::String("test".to_string())),
        );
        params_valid.insert("priority".to_string(), Box::new(Value::Integer(3)));

        let result = precondition(&params_valid);
        assert!(
            result.is_ok(),
            "Precondition should pass for valid priority"
        );
        assert_eq!(
            result.unwrap(),
            true,
            "Precondition should return true for priority 3"
        );

        // Test invalid priority (too low)
        let mut params_low: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_low.insert(
            "id".to_string(),
            Box::new(Value::String("test".to_string())),
        );
        params_low.insert("priority".to_string(), Box::new(Value::Integer(0)));

        let result_low = precondition(&params_low);
        assert!(result_low.is_ok(), "Precondition should not error");
        assert_eq!(
            result_low.unwrap(),
            false,
            "Precondition should return false for priority 0"
        );

        // Test invalid priority (too high)
        let mut params_high: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_high.insert(
            "id".to_string(),
            Box::new(Value::String("test".to_string())),
        );
        params_high.insert("priority".to_string(), Box::new(Value::Integer(6)));

        let result_high = precondition(&params_high);
        assert!(result_high.is_ok(), "Precondition should not error");
        assert_eq!(
            result_high.unwrap(),
            false,
            "Precondition should return false for priority 6"
        );
    }

    #[test]
    fn test_precondition_with_bool_parameter() {
        let ops = __operations_test_trait::test_trait("test-entity", "test_table", "id");

        // Test set_flag operation
        let set_flag_op = ops.iter().find(|op| op.name == "set_flag").unwrap();
        let precondition = set_flag_op.precondition.as_ref().unwrap();

        // Test with true value
        let mut params_true: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_true.insert(
            "id".to_string(),
            Box::new(Value::String("test".to_string())),
        );
        params_true.insert("value".to_string(), Box::new(Value::Boolean(true)));

        let result_true = precondition(&params_true);
        assert!(result_true.is_ok(), "Precondition should pass for true");
        assert_eq!(
            result_true.unwrap(),
            true,
            "Precondition should return true"
        );

        // Test with false value
        let mut params_false: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_false.insert(
            "id".to_string(),
            Box::new(Value::String("test".to_string())),
        );
        params_false.insert("value".to_string(), Box::new(Value::Boolean(false)));

        let result_false = precondition(&params_false);
        assert!(result_false.is_ok(), "Precondition should pass for false");
        assert_eq!(
            result_false.unwrap(),
            true,
            "Precondition should return true for false (it's a valid bool)"
        );
    }

    #[test]
    fn test_precondition_missing_parameter() {
        let ops = __operations_test_trait::test_trait("test-entity", "test_table", "id");

        let delete_op = ops.iter().find(|op| op.name == "delete").unwrap();
        let precondition = delete_op.precondition.as_ref().unwrap();

        // Test with missing parameter
        let params_missing: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        // Don't add "id" parameter

        let result = precondition(&params_missing);
        assert!(
            result.is_err(),
            "Precondition should error when required parameter is missing"
        );
        assert!(
            result.unwrap_err().contains("Missing"),
            "Error should mention missing parameter"
        );
    }

    #[test]
    fn test_precondition_boundary_values() {
        let ops = __operations_test_trait::test_trait("test-entity", "test_table", "id");

        let set_priority_op = ops.iter().find(|op| op.name == "set_priority").unwrap();
        let precondition = set_priority_op.precondition.as_ref().unwrap();

        // Test boundary values (1 and 5 should pass)
        let mut params_min: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_min.insert(
            "id".to_string(),
            Box::new(Value::String("test".to_string())),
        );
        params_min.insert("priority".to_string(), Box::new(Value::Integer(1)));

        let result_min = precondition(&params_min);
        assert!(
            result_min.is_ok(),
            "Precondition should pass for priority 1 (lower bound)"
        );
        assert_eq!(
            result_min.unwrap(),
            true,
            "Precondition should return true for priority 1"
        );

        let mut params_max: HashMap<String, Box<dyn Any + Send + Sync>> = HashMap::new();
        params_max.insert(
            "id".to_string(),
            Box::new(Value::String("test".to_string())),
        );
        params_max.insert("priority".to_string(), Box::new(Value::Integer(5)));

        let result_max = precondition(&params_max);
        assert!(
            result_max.is_ok(),
            "Precondition should pass for priority 5 (upper bound)"
        );
        assert_eq!(
            result_max.unwrap(),
            true,
            "Precondition should return true for priority 5"
        );
    }
}
