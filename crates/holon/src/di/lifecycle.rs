//! App lifecycle functions: creating and initializing BackendEngine via DI.

use anyhow::Result;
use ferrous_di::ServiceCollection;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::api::backend_engine::BackendEngine;
use crate::storage::schema_modules::create_core_schema_registry;
use crate::storage::turso::TursoBackend;

use super::STARTUP_QUERIES;
use super::registration::register_core_services_with_backend;

// Re-export Resolver trait for use in test code with create_backend_engine_with_extras
pub use ferrous_di::Resolver as DiResolver;

/// Pre-create materialized views for startup queries.
///
/// Call this during initialization, AFTER schema modules have been initialized via SchemaRegistry
/// but BEFORE file watching or data sync starts.
///
/// This function is idempotent - safe to call multiple times.
/// Views that already exist are skipped.
pub async fn preload_startup_views(
    engine: &BackendEngine,
    additional_queries: Option<&[&str]>,
) -> Result<()> {
    eprintln!(
        "[DI] preload_startup_views: starting with {} common queries",
        STARTUP_QUERIES.len()
    );

    engine.preload_views(STARTUP_QUERIES).await?;

    if let Some(queries) = additional_queries {
        eprintln!(
            "[DI] preload_startup_views: preloading {} additional queries",
            queries.len()
        );
        engine.preload_views(queries).await?;
    }

    eprintln!("[DI] preload_startup_views: completed");
    Ok(())
}

/// Shared setup function for creating BackendEngine with DI.
///
/// Sets up the DI container and returns a BackendEngine. Can be used by both TUI and Flutter.
pub async fn create_backend_engine<F>(db_path: PathBuf, setup_fn: F) -> Result<Arc<BackendEngine>>
where
    F: FnOnce(&mut ServiceCollection) -> Result<()>,
{
    eprintln!("[DI] Opening database at {:?}...", db_path);
    let db = TursoBackend::open_database(&db_path).expect("Failed to open database");
    eprintln!("[DI] Database opened successfully");

    let (cdc_tx, _) = tokio::sync::broadcast::channel(1024);

    eprintln!("[DI] Creating TursoBackend...");
    let (backend_inner, db_handle) =
        TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");
    eprintln!("[DI] TursoBackend created");

    let backend = Arc::new(RwLock::new(backend_inner));

    let mut services = ServiceCollection::new();

    register_core_services_with_backend(
        &mut services,
        db_path,
        backend.clone(),
        db_handle.clone(),
    )?;

    setup_fn(&mut services)?;

    eprintln!("[DI] Initializing schema modules via SchemaRegistry...");
    let registry = create_core_schema_registry();
    registry
        .initialize_all(backend.clone(), &db_handle, vec![])
        .await
        .expect("Failed to initialize schema modules");
    eprintln!("[DI] Schema modules initialized successfully");

    eprintln!("[DI] About to build ServiceProvider");
    let provider = services.build();
    eprintln!("[DI] ServiceProvider built successfully");

    eprintln!("[DI] Attempting to resolve BackendEngine...");
    let engine = DiResolver::get_required::<BackendEngine>(&provider);
    eprintln!("[DI] BackendEngine resolved successfully");

    Ok(engine)
}

/// Create a BackendEngine and resolve additional services from DI.
///
/// Like `create_backend_engine` but allows resolving additional services
/// from the DI container after the engine is created.
pub async fn create_backend_engine_with_extras<F, G, T>(
    db_path: PathBuf,
    setup_fn: F,
    extra_resolve: G,
) -> Result<(Arc<BackendEngine>, T)>
where
    F: FnOnce(&mut ServiceCollection) -> Result<()>,
    G: FnOnce(&ferrous_di::ServiceProvider) -> T,
{
    eprintln!("[DI] Opening database at {:?}...", db_path);
    let db = TursoBackend::open_database(&db_path).expect("Failed to open database");
    eprintln!("[DI] Database opened successfully");

    let (cdc_tx, _) = tokio::sync::broadcast::channel(1024);

    eprintln!("[DI] Creating TursoBackend...");
    let (backend_inner, db_handle) =
        TursoBackend::new(db, cdc_tx).expect("Failed to create TursoBackend");
    eprintln!("[DI] TursoBackend created");

    let backend = Arc::new(RwLock::new(backend_inner));

    let mut services = ServiceCollection::new();

    register_core_services_with_backend(
        &mut services,
        db_path,
        backend.clone(),
        db_handle.clone(),
    )?;

    setup_fn(&mut services)?;

    eprintln!("[DI] Initializing schema modules via SchemaRegistry...");
    let registry = create_core_schema_registry();
    registry
        .initialize_all(backend.clone(), &db_handle, vec![])
        .await
        .expect("Failed to initialize schema modules");
    eprintln!("[DI] Schema modules initialized successfully");

    eprintln!("[DI] About to build ServiceProvider");
    let provider = services.build();
    eprintln!("[DI] ServiceProvider built successfully");

    let extra = extra_resolve(&provider);

    eprintln!("[DI] Attempting to resolve BackendEngine...");
    let engine = DiResolver::get_required::<BackendEngine>(&provider);
    eprintln!("[DI] BackendEngine resolved successfully");

    Ok((engine, extra))
}
