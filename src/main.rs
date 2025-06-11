#![feature(iter_map_windows)]
use taxi_ai::App;
use tracing::{Level, info, subscriber::set_global_default};
use tracing_subscriber::fmt::{format::FmtSpan, time::SystemTime};

fn main() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_span_events(FmtSpan::ENTER)
        .with_thread_names(true)
        .with_line_number(true)
        .with_timer(SystemTime)
        .with_ansi(true)
        .pretty()
        .finish();

    set_global_default(subscriber).unwrap();
    console_error_panic_hook::set_once();
    info!("starting app...");
    dioxus::launch(App);
}
