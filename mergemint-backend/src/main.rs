// mergemint-backend/src/main.rs
//
// Application entry-point: builds the Axum router with middleware and starts
// the HTTP server.
//
// ## Request body size limits and timeout middleware (#476)
//
// Two middleware layers are added to the router to protect the service from
// slow clients and excessively large payloads:
//
//   * `RequestBodyLimitLayer` — rejects bodies larger than `MAX_BODY_BYTES`.
//     Without this, a malicious client could stream an arbitrarily large body
//     and exhaust server memory before any handler logic runs.
//
//   * `TimeoutLayer` — cancels any request (including body reads and handler
//     execution) that takes longer than `REQUEST_TIMEOUT`.  This prevents slow
//     clients or downstream Horizon calls from holding connections indefinitely
//     and starving the thread pool.
//
// ## Request correlation IDs (#486)
//
// Every inbound request is stamped with a UUID v4 correlation ID by
// `SetRequestIdLayer`.  The ID is read from the `x-request-id` header when
// present (so callers can propagate their own trace ID), or generated fresh
// when absent.  `TraceLayer` then opens a tracing span for each request that
// includes the correlation ID, making it trivial to grep logs for a single
// user's flow even when requests are interleaved.
//
// ## Graceful shutdown
//
// `axum::serve` is wired to `shutdown_signal`, which waits for SIGINT
// (Ctrl+C) or, on Unix, SIGTERM. Once either fires, Axum stops accepting new
// connections but lets in-flight requests finish — including a `self_claim`
// / `resolve_dispute` call that has already reached the point of submitting
// a chain transaction — instead of dropping them mid-flight when a deploy
// sends SIGTERM.
//
// ## Health checks
//
// `GET /health` returns `200 ok` once the server is listening. The runtime
// container image is distroless (no shell or curl), so the binary doubles as
// its own probe: `mergemint-backend healthcheck` requests `/health` on the
// local listener and exits non-zero if it doesn't get a 200.
//
// ## API docs (#866)
//
// When `ENABLE_DOCS=true`, Swagger UI is mounted at `/docs` so contributors
// and integrators can browse and try the API.  The flag defaults to off, so
// the docs are available locally and on staging without being exposed in
// production by default.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    http::{header::CONTENT_TYPE, HeaderValue, Method},
    routing::{get, post},
    Router,
};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::Level;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

mod db;
mod rate_limit;
mod routes;

use db::{new_shared_db, new_shared_idempotency_store};
use routes::bounties::{get_bounty_route, bounty_stream, claim_bounty, list_bounties, list_bounties_by_assignee};
use routes::tx::{new_shared_rate_limiter, resolve_dispute, self_claim, AppState};

/// Maximum allowed request body size (1 MiB).
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Maximum wall-clock time allowed for a single request, including body reads
/// and handler execution.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The canonical header name used to carry the correlation ID across service
/// boundaries.  Clients may supply their own value; if absent a UUID v4 is
/// generated automatically by `SetRequestIdLayer`.
const REQUEST_ID_HEADER: &str = "x-request-id";

/// Address the HTTP server listens on.
const LISTEN_ADDR: &str = "0.0.0.0:8080";

/// Address the `healthcheck` subcommand probes.
const HEALTHCHECK_ADDR: &str = "127.0.0.1:8080";

/// Reward-token allowlist env var consumed by create-bounty flows.
const ALLOWLISTED_REWARD_TOKENS_ENV: &str = "ALLOWLISTED_REWARD_TOKENS";

/// Env var holding a comma-separated allow-list of origins permitted to make
/// cross-origin requests, e.g.
/// "https://app.mergemint.xyz,https://staging.mergemint.xyz".
const CORS_ALLOWED_ORIGINS_ENV: &str = "CORS_ALLOWED_ORIGINS";

/// Env var that gates the Swagger UI at `/docs` (#866).  Docs are only served
/// when this is set to `true`, so they stay off in production by default.
const ENABLE_DOCS_ENV: &str = "ENABLE_DOCS";

/// Returns `true` when the `ENABLE_DOCS` env var is set to `true`
/// (case-insensitive).  Any other value — including unset — disables docs.
fn docs_enabled() -> bool {
    std::env::var(ENABLE_DOCS_ENV)
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        std::process::exit(match healthcheck(HEALTHCHECK_ADDR) {
            Ok(()) => 0,
            Err(err) => {
                eprintln!("healthcheck failed: {err}");
                1
            }
        });
    }

    // ---------------------------------------------------------------------------
    // Initialise structured logging (#486)
    //
    // We use a layered subscriber so that:
    //  - RUST_LOG (or the compiled-in default "info") controls the verbosity.
    //  - Each log line is emitted as JSON-friendly structured output so the
    //    correlation ID that TraceLayer injects into the span is visible in
    //    every downstream log record without extra formatting work.
    // ---------------------------------------------------------------------------
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            // Default: info-level for our crate, warn for noisy deps.
            "mergemint_backend=info,tower_http=debug,axum::rejection=trace"
                .parse()
                .unwrap()
        }))
        .with(fmt::layer())
        .init();

    warn_if_reward_token_allowlist_empty();

    let shared_db = new_shared_db();
    let idempotency = new_shared_idempotency_store();
    let (bounty_broadcast, _) = tokio::sync::broadcast::channel(100);
    let state = Arc::new(AppState {
        db: shared_db,
        idempotency,
        rate_limiter: new_shared_rate_limiter(),
        bounty_broadcast,
    });

    let mut app = Router::new()
        .route("/health", get(health))
        .route("/tx/resolve-dispute", post(resolve_dispute))
        .route("/tx/self-claim", post(self_claim))
        .route("/bounties", get(list_bounties))
        .route("/bounties/:id", get(get_bounty_route))
        .route(
            "/bounties/assignee/:address",
            get(list_bounties_by_assignee),
        )
        // ── Bounty push channel (#482) ─────────────────────────────────────
        .route("/bounties/:id/claim", post(claim_bounty))
        .route("/bounties/stream", get(bounty_stream))
        .with_state(state);

    // ── Swagger UI (#866) ────────────────────────────────────────────────
    //
    // Mounted at `/docs` only when `ENABLE_DOCS=true`, so the docs are
    // available locally and on staging without being exposed in production
    // by default.
    if docs_enabled() {
        app = app.merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()));
        tracing::info!("Swagger UI enabled at /docs");
    }

    let app = app
        // ── Correlation-ID middleware stack (#486) ──────────────────────────
        //
        // Layer order (innermost → outermost when receiving a request):
        //
        //  1. SetRequestIdLayer    — assigns x-request-id to every request that
        //                            does not already carry one.
        //  2. PropagateRequestIdLayer — copies the (possibly pre-existing)
        //                              x-request-id header into the response so
        //                              callers can correlate their own logs.
        //  3. TraceLayer           — opens a `tower_http::trace` span per
        //                            request; because it runs after the ID has
        //                            been set, the span automatically records
        //                            the correlation ID via the header extractor.
        //
        // NOTE: `.layer()` calls are applied bottom-up in Axum, so the layer
        // listed first in the source is closest to the handler.
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                let request_id = request
                    .headers()
                    .get(REQUEST_ID_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unknown");
                tracing::span!(
                    Level::INFO,
                    "request",
                    request_id = %request_id,
                    method    = %request.method(),
                    uri       = %request.uri(),
                )
            }),
        )
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        // ── Body / timeout guards (#476) ──────────────────────────────────

/* … truncated 8703 chars — edit only what you need near the top … */
