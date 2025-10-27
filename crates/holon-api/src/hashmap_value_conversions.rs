//! Conversions between HashMap types and Value.
//!
//! These implementations enable the Entity macro to work with HashMap fields
//! directly, with automatic JSON serialization to/from TEXT in the database.

use crate::Value;
use std::collections::HashMap;

// ============================================================================
// Required trait implementations for HashMap<String, String> to work with Entity macro
// ============================================================================

impl From<HashMap<String, String>> for Value {
    fn from(map: HashMap<String, String>) -> Self {
        Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect(),
        )
    }
}

impl TryFrom<Value> for HashMap<String, String> {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Object(obj) => obj
                .into_iter()
                .map(|(k, v)| match v {
                    Value::String(s) => Ok((k, s)),
                    _ => Err(format!("Value for key '{}' is not a string", k).into()),
                })
                .collect(),
            Value::Null => Ok(HashMap::new()),
            _ => Err("Value is not an object".into()),
        }
    }
}

// ============================================================================
// Required trait implementations for HashMap<String, Value> to work with Entity macro
// ============================================================================

impl TryFrom<Value> for HashMap<String, Value> {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Object(obj) => Ok(obj),
            Value::Null => Ok(HashMap::new()),
            _ => Err("Value is not an object".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::HasSchema;
    use holon_macros::Entity;
    use serde::{Deserialize, Serialize};

    /// Test entity with HashMap<String, String> properties field
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Entity)]
    #[entity(name = "test_blocks", api_crate = "crate")]
    struct TestBlockWithHashMap {
        #[primary_key]
        pub id: String,
        pub content: String,
        #[serde(default)]
        pub properties: HashMap<String, String>,
    }

    #[test]
    fn entity_macro_with_hashmap_to_entity() {
        let block = TestBlockWithHashMap {
            id: "test-1".to_string(),
            content: "Hello".to_string(),
            properties: [
                ("TODO".to_string(), "true".to_string()),
                ("PRIORITY".to_string(), "A".to_string()),
            ]
            .into_iter()
            .collect(),
        };

        // Convert to DynamicEntity (what gets stored)
        let entity = block.to_entity();

        // Check that properties are stored correctly
        let props_value = entity.get("properties").expect("properties field missing");
        println!("Stored as: {:?}", props_value);

        // Verify it's an Object variant
        assert!(matches!(props_value, Value::Object(_)));

        if let Value::Object(obj) = props_value {
            assert_eq!(obj.get("TODO"), Some(&Value::String("true".to_string())));
            assert_eq!(obj.get("PRIORITY"), Some(&Value::String("A".to_string())));
        }
    }

    #[test]
    fn entity_macro_with_hashmap_from_entity() {
        // Simulate what comes from the database
        let mut entity = crate::DynamicEntity::new("test_blocks");
        entity.set("id", "test-1");
        entity.set("content", "Hello");
        entity.set(
            "properties",
            Value::Object(
                [
                    ("TODO".to_string(), Value::String("true".to_string())),
                    ("PRIORITY".to_string(), Value::String("A".to_string())),
                ]
                .into_iter()
                .collect(),
            ),
        );

        // Convert from DynamicEntity
        let block = TestBlockWithHashMap::from_entity(entity).expect("conversion failed");

        assert_eq!(block.id, "test-1");
        assert_eq!(block.content, "Hello");
        assert_eq!(block.properties.get("TODO"), Some(&"true".to_string()));
        assert_eq!(block.properties.get("PRIORITY"), Some(&"A".to_string()));
    }

    #[test]
    fn entity_macro_schema_for_hashmap() {
        let schema = TestBlockWithHashMap::schema();

        println!("Table: {}", schema.table_name);
        for field in &schema.fields {
            println!("  Field: {} ({})", field.name, field.sql_type);
        }

        // Find properties field
        let props_field = schema.fields.iter().find(|f| f.name == "properties");
        assert!(props_field.is_some());

        // Should be TEXT (JSON stored as text in SQLite)
        assert_eq!(props_field.unwrap().sql_type, "TEXT");
    }

    #[test]
    fn roundtrip_entity_conversion() {
        let original = TestBlockWithHashMap {
            id: "test-1".to_string(),
            content: "Hello World".to_string(),
            properties: [
                ("SCHEDULED".to_string(), "<2024-01-15>".to_string()),
                ("CATEGORY".to_string(), "work".to_string()),
            ]
            .into_iter()
            .collect(),
        };

        // to_entity -> from_entity roundtrip
        let entity = original.to_entity();
        let restored = TestBlockWithHashMap::from_entity(entity).expect("roundtrip failed");

        assert_eq!(original, restored);
        println!("ROUNDTRIP VALIDATED: HashMap<String, String> works with Entity macro!");
    }

    #[test]
    fn h1_from_hashmap_string_string_to_value() {
        let map: HashMap<String, String> = [
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]
        .into_iter()
        .collect();

        let value: Value = map.into();
        assert!(matches!(value, Value::Object(_)));

        if let Value::Object(obj) = &value {
            assert_eq!(obj.get("key1"), Some(&Value::String("value1".to_string())));
        }
    }

    #[test]
    fn h2_tryfrom_value_to_hashmap_string_string() {
        let value = Value::Object(
            [
                ("key1".to_string(), Value::String("value1".to_string())),
                ("key2".to_string(), Value::String("value2".to_string())),
            ]
            .into_iter()
            .collect(),
        );

        let map: HashMap<String, String> = value.try_into().expect("conversion failed");
        assert_eq!(map.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn h3_json_serialization_roundtrip() {
        let map: HashMap<String, String> = [
            ("TODO".to_string(), "true".to_string()),
            ("PRIORITY".to_string(), "A".to_string()),
        ]
        .into_iter()
        .collect();

        let json = serde_json::to_string(&map).unwrap();
        println!("JSON: {}", json);

        let restored: HashMap<String, String> = serde_json::from_str(&json).unwrap();
        assert_eq!(map, restored);
    }

    /// Test entity with #[jsonb] attribute on properties field
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Entity)]
    #[entity(name = "jsonb_test_blocks", api_crate = "crate")]
    struct TestBlockWithJsonb {
        #[primary_key]
        pub id: String,
        pub content: String,
        #[serde(default)]
        #[jsonb]
        pub properties: String,
    }

    #[test]
    fn jsonb_attribute_sets_is_jsonb_flag() {
        let schema = TestBlockWithJsonb::schema();

        // Find properties field
        let props_field = schema.fields.iter().find(|f| f.name == "properties");
        assert!(
            props_field.is_some(),
            "properties field should exist in schema"
        );

        let props = props_field.unwrap();
        assert!(props.is_jsonb, "properties field should have is_jsonb=true");
        assert_eq!(
            props.sql_type, "TEXT",
            "JSONB fields should still be TEXT in SQLite"
        );

        // content field should NOT be jsonb
        let content_field = schema.fields.iter().find(|f| f.name == "content");
        assert!(content_field.is_some());
        assert!(
            !content_field.unwrap().is_jsonb,
            "content field should have is_jsonb=false"
        );
    }

    #[test]
    fn schema_field_is_jsonb_helper() {
        let schema = TestBlockWithJsonb::schema();

        assert!(
            schema.field_is_jsonb("properties"),
            "properties should be JSONB"
        );
        assert!(
            !schema.field_is_jsonb("content"),
            "content should not be JSONB"
        );
        assert!(!schema.field_is_jsonb("id"), "id should not be JSONB");
        assert!(
            !schema.field_is_jsonb("nonexistent"),
            "nonexistent field should return false"
        );
    }

    /// Test entity with Value type for jsonb field
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Entity)]
    #[entity(name = "value_field_test", api_crate = "crate")]
    struct TestBlockWithValueField {
        #[primary_key]
        pub id: String,
        #[jsonb]
        pub data: Value,
        #[jsonb]
        pub optional_data: Option<Value>,
    }

    #[test]
    fn jsonb_with_value_type_to_entity() {
        let block = TestBlockWithValueField {
            id: "test-1".to_string(),
            data: Value::Object(
                [
                    ("key".to_string(), Value::String("value".to_string())),
                    ("count".to_string(), Value::Integer(42)),
                ]
                .into_iter()
                .collect(),
            ),
            optional_data: Some(Value::Array(vec![Value::Integer(1), Value::Integer(2)])),
        };

        let entity = block.to_entity();

        // Check data field is stored correctly
        let data_value = entity.get("data").expect("data field missing");
        assert!(matches!(data_value, Value::Object(_)));

        // Check optional_data is stored correctly
        let opt_value = entity
            .get("optional_data")
            .expect("optional_data field missing");
        assert!(matches!(opt_value, Value::Array(_)));
    }

    #[test]
    fn jsonb_with_value_type_from_entity() {
        let mut entity = crate::DynamicEntity::new("value_field_test");
        entity.set("id", "test-1");
        entity.set(
            "data",
            Value::Object(
                [("nested".to_string(), Value::Boolean(true))]
                    .into_iter()
                    .collect(),
            ),
        );
        entity.set("optional_data", Value::Null);

        let block = TestBlockWithValueField::from_entity(entity).expect("conversion failed");

        assert_eq!(block.id, "test-1");
        if let Value::Object(obj) = &block.data {
            assert_eq!(obj.get("nested"), Some(&Value::Boolean(true)));
        } else {
            panic!("data should be Object");
        }
        // Note: Value::Null converts to Some(Value::Null) for Option<Value>
        // because Value::Null is a valid Value variant
        assert_eq!(block.optional_data, Some(Value::Null));
    }

    #[test]
    fn jsonb_with_value_type_roundtrip() {
        // Test with Some(value)
        let original = TestBlockWithValueField {
            id: "test-1".to_string(),
            data: Value::Object(
                [(
                    "complex".to_string(),
                    Value::Array(vec![
                        Value::String("a".to_string()),
                        Value::Integer(1),
                        Value::Boolean(false),
                    ]),
                )]
                .into_iter()
                .collect(),
            ),
            optional_data: Some(Value::String("present".to_string())),
        };

        let entity = original.to_entity();
        let restored = TestBlockWithValueField::from_entity(entity).expect("roundtrip failed");

        assert_eq!(original, restored);
    }

    #[test]
    fn jsonb_with_value_none_becomes_null() {
        // When optional_data is None, it gets stored as Value::Null
        // and comes back as Some(Value::Null) - this is expected for Option<Value>
        let original = TestBlockWithValueField {
            id: "test-1".to_string(),
            data: Value::Integer(42),
            optional_data: None,
        };

        let entity = original.to_entity();
        let opt_value = entity.get("optional_data").expect("field should exist");
        assert!(opt_value.is_null(), "None should be stored as Value::Null");

        let restored = TestBlockWithValueField::from_entity(entity).expect("roundtrip failed");
        // None becomes Some(Null) for Option<Value> - this is correct behavior
        assert_eq!(restored.optional_data, Some(Value::Null));
    }
}
