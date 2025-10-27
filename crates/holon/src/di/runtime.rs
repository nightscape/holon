//! Runtime utilities for bridging async/sync boundaries in DI factories.

use std::future::Future;
use std::sync::Arc;
use tokio::sync::RwLock;

use ferrous_di::Resolver;

use crate::core::queryable_cache::QueryableCache;
use crate::core::traits::HasSchema;
use crate::storage::turso::TursoBackend;

use super::TursoBackendProvider;

/// Runs an async operation in a synchronous DI factory context.
///
/// DI factories are synchronous, but many services require async initialization.
/// This helper tries to stay on the current runtime (to communicate with actors
/// spawned there) but falls back to a new thread if no runtime is available.
///
/// Strategy:
/// 1. If inside a multi-threaded tokio runtime: use block_in_place + Handle::current()
/// 2. If inside a current-thread runtime: use Handle::current() directly (may block)
/// 3. If no runtime: spawn a new thread with its own runtime
///
/// IMPORTANT for Flutter: When called during DI resolution within an async context
/// (like FrontendSession::new), we're already on FRB's runtime where the actor lives,
/// so we MUST stay on that runtime to communicate with the actor.
pub fn run_async_in_sync_factory<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => handle.block_on(future),
            _ => tokio::task::block_in_place(|| handle.block_on(future)),
        },
        Err(_) => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                std::thread::spawn(move || {
                    let rt =
                        tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                    rt.block_on(future)
                })
                .join()
                .expect("Thread panicked during async initialization")
            }

            #[cfg(target_arch = "wasm32")]
            {
                panic!("run_async_in_sync_factory called outside async context on WASM");
            }
        }
    }
}

/// Creates a QueryableCache for a given type using the TursoBackend from DI.
///
/// This function uses trait-based resolution (`TursoBackendProvider`) to avoid
/// TypeId mismatches when called from different compilation units.
pub fn create_queryable_cache<T, R: Resolver>(resolver: &R) -> QueryableCache<T>
where
    T: HasSchema + Send + Sync + 'static,
{
    eprintln!(
        "[DI] create_queryable_cache<{}> called",
        std::any::type_name::<T>()
    );
    eprintln!("[DI] create_queryable_cache: resolving TursoBackendProvider trait...");
    let provider = resolver.get_required_trait::<dyn TursoBackendProvider>();
    let backend = provider.backend();
    eprintln!("[DI] create_queryable_cache: TursoBackendProvider resolved, backend obtained");

    create_queryable_cache_with_backend(backend)
}

/// Resolves the TursoBackend from DI container.
///
/// **IMPORTANT**: This is a non-generic function to ensure TypeId computation happens
/// within the `holon` crate's compilation unit. Generic functions get monomorphized
/// in the calling crate, which can cause TypeId mismatches when using `Any::downcast`.
pub fn resolve_turso_backend(resolver: &impl Resolver) -> Arc<RwLock<TursoBackend>> {
    resolver.get_required::<RwLock<TursoBackend>>()
}

/// Creates a QueryableCache for a given type using a pre-resolved TursoBackend.
///
/// Useful for avoiding TypeId mismatches when called from different compilation units.
/// The caller should resolve the backend using `resolve_turso_backend` (which is non-generic)
/// and pass it to this function.
pub fn create_queryable_cache_with_backend<T>(
    backend: Arc<RwLock<TursoBackend>>,
) -> QueryableCache<T>
where
    T: HasSchema + Send + Sync + 'static,
{
    eprintln!(
        "[DI] create_queryable_cache_with_backend<{}> called",
        std::any::type_name::<T>()
    );

    run_async_in_sync_factory(async move {
        QueryableCache::new(backend.clone())
            .await
            .expect("Failed to create QueryableCache")
    })
}
