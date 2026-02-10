mod cdc;
mod operations;
mod render;
mod state;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use blinc_app::prelude::*;
use blinc_app::windowed::WindowedApp;
use blinc_theme::{ColorToken, ThemeState};

use blinc_core::State;
use holon_frontend::{FrontendConfig, FrontendSession};

use render::context::RenderContext;
use render::interpreter::interpret;
use state::AppState;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "holon_blinc=info,holon=info".into()),
        )
        .init();

    let config = parse_args()?;

    eprintln!(
        "Blinc frontend: db={}, orgmode={:?}, loro={}",
        config
            .db_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or("(temp)".into()),
        config.orgmode_root,
        config.loro_enabled
    );

    let runtime = tokio::runtime::Runtime::new()?;

    let (session, app_state) = runtime.block_on(async {
        let frontend_config = build_frontend_config(&config);
        tracing::info!("Starting Blinc frontend...");
        let session = Arc::new(FrontendSession::new(frontend_config).await?);

        // Start MCP server on port 8520 (same as Flutter)
        {
            let mcp_engine = session.engine().clone();
            let mcp_port: u16 = std::env::var("MCP_SERVER_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8520);
            let bind_address = std::net::SocketAddr::from(([127, 0, 0, 1], mcp_port));
            let cancellation_token = tokio_util::sync::CancellationToken::new();
            tracing::info!("Starting MCP server on http://{}", bind_address);
            tokio::spawn(async move {
                if let Err(e) =
                    holon_mcp::di::run_http_server(mcp_engine, bind_address, cancellation_token)
                        .await
                {
                    tracing::error!("MCP server error: {}", e);
                }
            });
        }

        let app_state = match session.initial_widget().await {
            Ok((widget_spec, stream)) => {
                tracing::info!("Initial widget loaded: {} rows", widget_spec.data.len());
                let app_state = AppState::new(widget_spec);
                let cdc_state = app_state.clone_handle();
                tokio::spawn(cdc::cdc_listener(stream, cdc_state));
                app_state
            }
            Err(e) => {
                tracing::warn!("initial_widget() failed: {e}");
                tracing::info!("Starting with empty state - no root layout block found");
                let empty_spec = holon_api::widget_spec::WidgetSpec::new(
                    holon_api::render_types::RenderSpec {
                        root: holon_api::render_types::RenderExpr::Literal {
                            value: holon_api::Value::String(format!("Error: {e}")),
                        },
                        default_view: "default".to_string(),
                        views: Default::default(),
                        nested_queries: vec![],
                        operations: Default::default(),
                        row_templates: vec![],
                    },
                    vec![],
                );
                AppState::new(empty_spec)
            }
        };
        Ok::<_, anyhow::Error>((session, app_state))
    })?;

    // Keep the runtime alive in a background thread
    let rt_handle = runtime.handle().clone();
    let _runtime_guard = std::thread::spawn(move || {
        runtime.block_on(std::future::pending::<()>());
    });

    ThemeState::init_default();

    #[cfg(feature = "hot-reload")]
    {
        let patch_state = app_state.clone_handle();
        subsecond::register_handler(Arc::new(move || {
            patch_state.mark_dirty();
        }));
    }

    Ok(WindowedApp::run(WindowConfig::default(), move |ctx| {
        let sidebar_open: State<bool> = ctx.use_state_keyed("sidebar_open", || true);

        let widget_spec = app_state.widget_spec();
        let mut render_ctx = RenderContext::new(Arc::clone(&session), rt_handle.clone());
        render_ctx.is_screen_layout = true;
        render_ctx.sidebar_open = Some(sidebar_open.clone());

        let root = {
            #[cfg(feature = "hot-reload")]
            {
                subsecond::call(|| render_root(&widget_spec, &render_ctx))
            }
            #[cfg(not(feature = "hot-reload"))]
            {
                render_root(&widget_spec, &render_ctx)
            }
        };

        let theme = ThemeState::get();
        let title_bar = build_title_bar(&sidebar_open, theme);

        div()
            .w(ctx.width)
            .h(ctx.height)
            .bg(theme.color(ColorToken::Background))
            .flex_col()
            .child(title_bar)
            .child(div().flex_1().overflow_clip().child(root))
    })?)
}

struct CliConfig {
    db_path: Option<PathBuf>,
    orgmode_root: Option<PathBuf>,
    loro_enabled: bool,
}

fn parse_args() -> Result<CliConfig> {
    let mut args = std::env::args().skip(1);
    let mut db_path: Option<PathBuf> = std::env::var("HOLON_DB_PATH").ok().map(PathBuf::from);
    let mut orgmode_root: Option<PathBuf> =
        std::env::var("HOLON_ORGMODE_ROOT").ok().map(PathBuf::from);
    let mut loro_enabled = std::env::var("HOLON_LORO_ENABLED")
        .map(|v| !v.is_empty() && v != "0" && v.to_lowercase() != "false")
        .unwrap_or(false);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--orgmode-root" | "--orgmode-dir" => {
                let path_str = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--orgmode-root requires a path argument"))?;
                orgmode_root = Some(PathBuf::from(path_str));
            }
            "--loro" => {
                loro_enabled = true;
            }
            "--help" | "-h" => {
                eprintln!("Usage: holon-blinc [OPTIONS] [DATABASE_PATH]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --orgmode-root PATH  OrgMode root directory");
                eprintln!("  --loro               Enable Loro CRDT layer");
                eprintln!("  --help, -h           Show this help message");
                eprintln!();
                eprintln!("Environment variables:");
                eprintln!("  HOLON_DB_PATH          Database file path");
                eprintln!("  HOLON_ORGMODE_ROOT     OrgMode root directory");
                eprintln!("  HOLON_LORO_ENABLED     Enable Loro CRDT (1/true)");
                eprintln!();
                eprintln!("Examples:");
                eprintln!("  holon-blinc /path/to/db.db --orgmode-root /path/to/org/files");
                eprintln!("  HOLON_DB_PATH=./holon.db HOLON_ORGMODE_ROOT=./pkm holon-blinc");
                std::process::exit(0);
            }
            _ => {
                if !arg.starts_with("--") {
                    db_path = Some(PathBuf::from(arg));
                }
            }
        }
    }

    Ok(CliConfig {
        db_path,
        orgmode_root,
        loro_enabled,
    })
}

fn build_frontend_config(cli: &CliConfig) -> FrontendConfig {
    let mut config = FrontendConfig::new().with_settle_delay(2000);

    if let Some(ref db) = cli.db_path {
        config = config.with_db_path(db.clone());
    }
    if let Some(ref org) = cli.orgmode_root {
        config = config.with_orgmode(org.clone());
    }
    if cli.loro_enabled {
        config = config.with_loro();
    }

    config
}

fn render_root(widget_spec: &holon_api::widget_spec::WidgetSpec, ctx: &RenderContext) -> Div {
    let root_expr = widget_spec.render_spec.root();

    match root_expr {
        Some(expr) => {
            let ctx = ctx.with_data_rows(widget_spec.data.clone());
            interpret(expr, &ctx)
        }
        None => div().flex_col().justify_center().items_center().child(
            text("No render spec available")
                .size(16.0)
                .color(ThemeState::get().color(ColorToken::TextSecondary)),
        ),
    }
}

const TITLE_BAR_HEIGHT: f32 = 32.0;

const HAMBURGER_SVG: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" x2="20" y1="12" y2="12"/><line x1="4" x2="20" y1="6" y2="6"/><line x1="4" x2="20" y1="18" y2="18"/></svg>"#;

const HAMBURGER_OPEN_SVG: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" x2="20" y1="12" y2="12"/><line x1="4" x2="14" y1="6" y2="6"/><line x1="4" x2="14" y1="18" y2="18"/></svg>"#;

fn build_title_bar(sidebar_open: &State<bool>, theme: &ThemeState) -> Div {
    let is_open = sidebar_open.get();
    let icon = if is_open {
        HAMBURGER_OPEN_SVG
    } else {
        HAMBURGER_SVG
    };

    let sidebar_state = sidebar_open.clone();

    div()
        .flex_row()
        .items_center()
        .w_full()
        .h(TITLE_BAR_HEIGHT)
        .bg(theme.color(ColorToken::Background))
        .border_bottom(1.0, theme.color(ColorToken::Border))
        .child(
            div()
                .flex_row()
                .items_center()
                .justify_center()
                .w(TITLE_BAR_HEIGHT)
                .h(TITLE_BAR_HEIGHT)
                .cursor(blinc_layout::element::CursorStyle::Pointer)
                .child(
                    svg(icon)
                        .size(18.0, 18.0)
                        .color(theme.color(ColorToken::TextSecondary)),
                )
                .on_click(move |_| {
                    sidebar_state.update_rebuild(|open| !open);
                }),
        )
        .child(
            text("Holon")
                .size(14.0)
                .color(theme.color(ColorToken::TextPrimary)),
        )
}
