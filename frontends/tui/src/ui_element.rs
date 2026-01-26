use holon_prql_render::OperationWiring;
use r3bl_tui::TuiColor;
use tracing::debug;

/// Intermediate representation of UI elements with operations attached.
/// This separates the interpretation phase (RenderSpec → UIElement)
/// from the rendering phase (UIElement → RenderOpIRVec).
#[derive(Debug, Clone)]
pub enum UIElement {
    Text {
        content: String,
        fg_color: Option<TuiColor>,
        bg_color: Option<TuiColor>,
    },
    EditableText {
        content: String,
        operations: Vec<OperationWiring>,
        fg_color: Option<TuiColor>,
        bg_color: Option<TuiColor>,
    },
    Checkbox {
        checked: bool,
        operations: Vec<OperationWiring>,
    },
    Badge {
        content: String,
        color: TuiColor,
    },
    Icon {
        symbol: String,
    },
    Row {
        children: Vec<UIElement>,
    },
}

impl UIElement {
    /// Get the first operation associated with this element, if any.
    /// For Row elements, searches children recursively to find a Checkbox or EditableText with operations.
    pub fn get_operation(&self) -> Option<&OperationWiring> {
        match self {
            UIElement::Checkbox { operations, .. } => operations.first(),
            UIElement::EditableText { operations, .. } => operations.first(),
            UIElement::Row { children } => {
                // Search children recursively for a checkbox or editable_text with operations
                for child in children {
                    if let Some(op) = child.get_operation() {
                        return Some(op);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Find the first EditableText child recursively, if any
    pub fn find_editable_text(&self) -> Option<&UIElement> {
        match self {
            UIElement::EditableText { .. } => Some(self),
            UIElement::Row { children } => {
                for child in children {
                    if let Some(editable) = child.find_editable_text() {
                        return Some(editable);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Find an operation descriptor by operation name (e.g., "outdent", "indent")
    pub fn find_operation_descriptor(
        &self,
        op_name: &str,
    ) -> Option<&holon_api::OperationDescriptor> {
        match self {
            UIElement::Checkbox { operations, .. } | UIElement::EditableText { operations, .. } => {
                debug!(
                    "Searching for '{}' in {} operations",
                    op_name,
                    operations.len()
                );
                for op in operations.iter() {
                    debug!(
                        "  Checking operation: name='{}', entity='{}'",
                        op.descriptor.name, op.descriptor.entity_name
                    );
                }
                operations
                    .iter()
                    .find(|op| op.descriptor.name == op_name)
                    .map(|op| &op.descriptor)
            }
            UIElement::Row { children } => {
                debug!(
                    "Searching Row with {} children for operation '{}'",
                    children.len(),
                    op_name
                );
                children
                    .iter()
                    .find_map(|child| child.find_operation_descriptor(op_name))
            }
            _ => {
                debug!(
                    "Element type {:?} has no operations",
                    std::mem::discriminant(self)
                );
                None
            }
        }
    }
}
