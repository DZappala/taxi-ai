#![feature(iter_map_windows)]
use chart::features::{chart, render};
use clap::Parser;
use clean::preproc;
use cmd::args::Args;
use tracing::{Level, subscriber::set_global_default};
use tracing_subscriber::fmt::{format::FmtSpan, time::SystemTime};

pub mod chart;
pub mod clean;
pub mod cmd;

fn main() {
    let args = Args::parse();
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

    let dfs = preproc(args.rebuild(), None);
    let jan_chart = chart(dfs.first().unwrap().df());
    render(jan_chart, "jan_chart");
}
