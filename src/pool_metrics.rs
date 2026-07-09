//! Optional OpenTelemetry connection-pool gauges for the MongoDB client, enabled
//! via `Config::enable_metrics`. The driver only exposes pool state as a push
//! stream of CMAP events (no `client.pool_size()` getter), so this bridges that
//! stream to OTel's pull-based observable gauges via a couple of shared atomics
//! updated on each event.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use mongodb::event::EventHandler;
use mongodb::event::cmap::CmapEvent;
use mongodb::options::ClientOptions;

pub(crate) fn install(options: &mut ClientOptions) {
    let pool_size = Arc::new(AtomicI64::new(0));
    let checked_out = Arc::new(AtomicI64::new(0));

    let event_pool_size = pool_size.clone();
    let event_checked_out = checked_out.clone();
    options.cmap_event_handler = Some(EventHandler::callback(move |event: CmapEvent| {
        match event {
            CmapEvent::ConnectionCreated(_) => {
                event_pool_size.fetch_add(1, Ordering::Relaxed);
            }
            CmapEvent::ConnectionClosed(_) => {
                event_pool_size.fetch_sub(1, Ordering::Relaxed);
            }
            CmapEvent::ConnectionCheckedOut(_) => {
                event_checked_out.fetch_add(1, Ordering::Relaxed);
            }
            CmapEvent::ConnectionCheckedIn(_) => {
                event_checked_out.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }));

    let meter = opentelemetry::global::meter("getitdone");
    let _ = meter
        .i64_observable_gauge("getitdone.mongo_pool.size")
        .with_description("Total connections currently open in the MongoDB client's pool")
        .with_callback(move |observer| observer.observe(pool_size.load(Ordering::Relaxed), &[]))
        .build();
    let _ = meter
        .i64_observable_gauge("getitdone.mongo_pool.checked_out")
        .with_description(
            "Connections currently checked out (in use) from the MongoDB client's pool",
        )
        .with_callback(move |observer| observer.observe(checked_out.load(Ordering::Relaxed), &[]))
        .build();
}
