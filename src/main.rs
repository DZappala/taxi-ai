#![feature(iter_map_windows)]
use chart::features::{chart, render};
use clean::preproc;
use tracing::{Level, subscriber::set_global_default};
use tracing_subscriber::fmt::{format::FmtSpan, init, time::SystemTime};

pub mod chart;
pub mod clean;

fn main() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_span_events(FmtSpan::FULL)
        .with_thread_names(true)
        .with_line_number(true)
        .with_timer(SystemTime)
        .with_ansi(true)
        .pretty()
        .finish();

    set_global_default(subscriber).unwrap();

    let dfs = preproc();
    let jan_chart = chart(dfs.first().unwrap().df());
    render(jan_chart, "jan_chart");
}
