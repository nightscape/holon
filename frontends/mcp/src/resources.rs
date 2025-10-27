use crate::server::HolonMcpServer;
use holon::core::datasource::OperationProvider;
use rmcp::model::*;
use rmcp::{service::RequestContext, ErrorData as McpError, RoleServer};

impl HolonMcpServer {
    pub async fn list_resources_impl(
        &self,
        _request: Option<PaginatedRequestParam>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        // Get all available entity names from operations
        let dispatcher = self.engine.get_dispatcher();
        let all_ops = OperationProvider::operations(&*dispatcher);
        let mut entity_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        for op in all_ops {
            entity_names.insert(op.entity_name.clone());
        }

        let mut resources =
            vec![
                RawResource::new("holon://operations", "Available Operations".to_string())
                    .no_annotation(),
            ];

        // Add a resource for each entity
        for entity_name in entity_names {
            resources.push(
                RawResource::new(
                    format!("holon://operations/{}", entity_name),
                    format!("Operations for {}", entity_name),
                )
                .no_annotation(),
            );
        }

        Ok(ListResourcesResult {
            resources,
            next_cursor: None,
            meta: None,
        })
    }

    pub async fn read_resource_impl(
        &self,
        ReadResourceRequestParam { uri }: ReadResourceRequestParam,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        if uri == "holon://operations" {
            // List all operations grouped by entity
            let dispatcher = self.engine.get_dispatcher();
            let all_ops = OperationProvider::operations(&*dispatcher);

            let mut ops_by_entity: std::collections::HashMap<
                String,
                Vec<&holon_api::OperationDescriptor>,
            > = std::collections::HashMap::new();
            for op in &all_ops {
                ops_by_entity
                    .entry(op.entity_name.clone())
                    .or_insert_with(Vec::new)
                    .push(op);
            }

            let json_value: serde_json::Value = ops_by_entity
                .iter()
                .map(|(entity_name, ops)| {
                    let ops_json: Vec<serde_json::Value> = ops.iter()
                        .map(|op| serde_json::json!({
                            "entity_name": op.entity_name,
                            "entity_short_name": op.entity_short_name,
                            "id_column": op.id_column,
                            "name": op.name,
                            "display_name": op.display_name,
                            "description": op.description,
                            "required_params": op.required_params.iter().map(|p| serde_json::json!({
                                "name": p.name,
                                "type_hint": format!("{:?}", p.type_hint),
                                "description": p.description,
                            })).collect::<Vec<_>>(),
                            "affected_fields": op.affected_fields,
                        }))
                        .collect();
                    (entity_name.clone(), serde_json::Value::Array(ops_json))
                })
                .collect();

            Ok(ReadResourceResult {
                contents: vec![ResourceContents::text(
                    serde_json::to_string_pretty(&json_value).map_err(|e| {
                        McpError::internal_error(
                            "serialization_failed",
                            Some(serde_json::json!({"error": e.to_string()})),
                        )
                    })?,
                    uri,
                )],
            })
        } else if uri.starts_with("holon://operations/") {
            let entity = uri.strip_prefix("holon://operations/").unwrap();
            let ops = self.engine.available_operations(entity).await;

            // Serialize ops to JSON
            let json_ops: Vec<serde_json::Value> = ops
                .iter()
                .map(|op| {
                    serde_json::json!({
                        "entity_name": op.entity_name,
                        "entity_short_name": op.entity_short_name,
                        "id_column": op.id_column,
                        "name": op.name,
                        "display_name": op.display_name,
                        "description": op.description,
                        "required_params": op.required_params.iter().map(|p| serde_json::json!({
                            "name": p.name,
                            "type_hint": format!("{:?}", p.type_hint),
                            "description": p.description,
                        })).collect::<Vec<_>>(),
                        "affected_fields": op.affected_fields,
                    })
                })
                .collect();

            Ok(ReadResourceResult {
                contents: vec![ResourceContents::text(
                    serde_json::to_string_pretty(&json_ops).map_err(|e| {
                        McpError::internal_error(
                            "serialization_failed",
                            Some(serde_json::json!({"error": e.to_string()})),
                        )
                    })?,
                    uri,
                )],
            })
        } else {
            Err(McpError::resource_not_found(
                "resource_not_found",
                Some(serde_json::json!({"uri": uri})),
            ))
        }
    }
}
