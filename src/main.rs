#![feature(iter_map_windows)]
use std::path::Path;

use chart::features::{chart, render};
use clean::{RawData, preproc_checked};
use itertools::Itertools;
use tracing::{Level, subscriber::set_global_default};
use tracing_subscriber::fmt::{format::FmtSpan, time::SystemTime};

pub mod chart;
pub mod clean;

pub const JAN_RAW: &str = "db/raw/yellow_tripdata_2020-01.parquet";
pub const MAR_RAW: &str = "db/raw/yellow_tripdata_2020-03.parquet";
pub const MAY_RAW: &str = "db/raw/yellow_tripdata_2020-05.parquet";

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

    let raw_taxi_data = [
        RawData::new("Jan", JAN_RAW),
        RawData::new("Mar", MAR_RAW),
        RawData::new("May", MAY_RAW),
    ];

    let zipped = raw_taxi_data
        .iter()
        .zip(raw_taxi_data.iter().map(|RawData { name, ent: _ }| {
            let path = format!("db/record/taxi_weather-{name}.csv");
            (Path::new(&path).exists(), path)
        }))
        .collect_vec();

    let dfs = preproc_checked(&zipped);
    dfs.iter().for_each(|df| {
        let c = chart(df.df());
        render(c, df.name())
    });
}
