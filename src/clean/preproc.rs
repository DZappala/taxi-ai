use std::path::Path;

use polars::prelude::*;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use tracing::{instrument, trace, trace_span};

use super::io::{
    JAN_RAW, MAR_RAW, MAY_RAW, NamedDataFrame, RawDataEntry, WEATHER_RAW, save_csv, scan_csv,
};

pub fn clean(lf: LazyFrame) -> PolarsResult<DataFrame> {
    trace_span!("clean");
    trace!("clean()");
    trace!("Select from dataframe.");
    lf.select([
        col("tpep_pickup_datetime").alias("time"),
        col("passenger_count"),
        col("trip_distance"),
        col("tolls_amount"),
        col("tpep_pickup_datetime").dt().hour().alias("pickup_hour"),
        col("tpep_pickup_datetime")
            .dt()
            .hour()
            .sin()
            .alias("pickup_hour_sin"),
        col("tpep_pickup_datetime")
            .dt()
            .hour()
            .cos()
            .alias("pickup_hour_cos"),
        col("tpep_pickup_datetime")
            .dt()
            .ordinal_day()
            .alias("pickup_day"),
        col("tpep_pickup_datetime")
            .dt()
            .ordinal_day()
            .sin()
            .alias("pickup_day_sin"),
        col("tpep_pickup_datetime")
            .dt()
            .ordinal_day()
            .cos()
            .alias("pickup_day_cos"),
        (col("tpep_dropoff_datetime") - col("tpep_pickup_datetime"))
            .dt()
            .total_seconds()
            .alias("total_seconds"),
    ])
    .drop_nans(None)
    .drop_nulls(None)
    .filter(col("total_seconds").gt(lit(0)))
    .filter(col("total_seconds").lt(lit(36000)))
    .collect()
}

#[instrument]
pub fn preproc<'a>() -> Vec<NamedDataFrame<'a>> {
    trace!("preproc()");
    trace!("Creating raw data entries.");
    let raw_taxi_data: Vec<RawDataEntry> = vec![
        RawDataEntry::new("Jan", JAN_RAW),
        RawDataEntry::new("Mar", MAR_RAW),
        RawDataEntry::new("May", MAY_RAW),
    ];

    trace!("scanning taxi data");
    let scanned_taxi_data: Vec<NamedDataFrame> = raw_taxi_data
        .into_par_iter()
        .filter_map(|RawDataEntry { name, ent }| {
            LazyFrame::scan_parquet(ent, ScanArgsParquet::default())
                .and_then(clean)
                .ok()
                .map(|df: DataFrame| NamedDataFrame { name, df })
        })
        .collect();

    assert!(scanned_taxi_data.len() == 3);

    trace!("scanning weather data.");
    let scanned_weather_data = scan_csv(Path::new(WEATHER_RAW))
        .unwrap_or_else(|e| panic!("Failed to parse weather data: {e}"));

    trace!("Joining taxi and weather data.");
    let mut joined_taxi_weather: Vec<NamedDataFrame> = scanned_taxi_data
        .par_iter()
        .map(|NamedDataFrame { name, df }| {
            df.join(
                &scanned_weather_data,
                ["time"],
                ["time_ns"],
                JoinArgs::new(JoinType::AsOf(AsOfOptions::default())),
                None,
            )
            .map(|v| NamedDataFrame::new(name, v.drop_many(["time", "time_ns"])))
            .unwrap_or_else(|e| panic!("Failed to join taxi and weather data; {e}"))
        })
        .collect();

    joined_taxi_weather.iter_mut().for_each(save_csv);
    joined_taxi_weather
}
