use holon_api::{OperationDescriptor, OperationParam, OperationWiring, TypeHint};
/// Integration tests for editable_text widget functionality
use tui_r3bl_frontend::UIElement;

// Helper function to extract field name from OperationWiring
fn get_field_name(op: &OperationWiring) -> String {
    op.modified_param.clone()
}

#[test]
fn test_editable_text_creation() {
    let operations = vec![OperationWiring {
        widget_type: "editable_text".to_string(),
        modified_param: "content".to_string(),
        descriptor: OperationDescriptor {
            entity_name: "block".to_string(),
            id_column: "id".to_string(),
            name: "set_field".to_string(),
            display_name: "Set field".to_string(),
            description: "Update field".to_string(),
            required_params: vec![
                OperationParam {
                    name: "id".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "Entity ID".to_string(),
                },
                OperationParam {
                    name: "field".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "Field name".to_string(),
                },
                OperationParam {
                    name: "value".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "New value".to_string(),
                },
            ],
            precondition: None,
        },
    }];

    let editable = UIElement::EditableText {
        content: "Hello, World!".to_string(),
        operations: operations.clone(),
        fg_color: None,
        bg_color: None,
    };

    assert!(editable.is_editable());
    assert_eq!(get_field_name(editable.get_operation().unwrap()), "content");
}

#[test]
fn test_editable_text_get_operation() {
    let operations = vec![OperationWiring {
        widget_type: "editable_text".to_string(),
        modified_param: "content".to_string(),
        descriptor: OperationDescriptor {
            entity_name: "block".to_string(),
            id_column: "id".to_string(),
            name: "set_field".to_string(),
            display_name: "Set field".to_string(),
            description: "Update field".to_string(),
            required_params: vec![
                OperationParam {
                    name: "id".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "Entity ID".to_string(),
                },
                OperationParam {
                    name: "field".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "Field name".to_string(),
                },
                OperationParam {
                    name: "value".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "New value".to_string(),
                },
            ],
            precondition: None,
        },
    }];

    let editable = UIElement::EditableText {
        content: "Test".to_string(),
        operations: operations.clone(),
        fg_color: None,
        bg_color: None,
    };

    let op = editable.get_operation();
    assert!(op.is_some());
    assert_eq!(get_field_name(op.unwrap()), "content");
}

#[test]
fn test_editable_text_in_row() {
    let operations = vec![OperationWiring {
        widget_type: "editable_text".to_string(),
        modified_param: "content".to_string(),
        descriptor: OperationDescriptor {
            entity_name: "block".to_string(),
            id_column: "id".to_string(),
            name: "set_field".to_string(),
            display_name: "Set field".to_string(),
            description: "Update field".to_string(),
            required_params: vec![
                OperationParam {
                    name: "id".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "Entity ID".to_string(),
                },
                OperationParam {
                    name: "field".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "Field name".to_string(),
                },
                OperationParam {
                    name: "value".to_string(),
                    type_hint: TypeHint::String.into(),
                    description: "New value".to_string(),
                },
            ],
            precondition: None,
        },
    }];

    let row = UIElement::Row {
        children: vec![
            UIElement::Text {
                content: "Prefix: ".to_string(),
                fg_color: None,
                bg_color: None,
            },
            UIElement::EditableText {
                content: "Editable content".to_string(),
                operations: operations.clone(),
                fg_color: None,
                bg_color: None,
            },
        ],
    };

    // Row should find the operation from its editable_text child
    let op = row.get_operation();
    assert!(op.is_some());
    assert_eq!(get_field_name(op.unwrap()), "content");
}

#[test]
fn test_is_editable() {
    let editable = UIElement::EditableText {
        content: "Test".to_string(),
        operations: vec![],
        fg_color: None,
        bg_color: None,
    };

    assert!(editable.is_editable());

    let text = UIElement::Text {
        content: "Test".to_string(),
        fg_color: None,
        bg_color: None,
    };

    assert!(!text.is_editable());
}
