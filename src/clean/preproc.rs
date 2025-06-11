use std::path::Path;

use super::io::{NamedData, RawData, WEATHER_RAW, load_csv, save_csv, scan_csv};
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tracing::{info, instrument};

fn clean(lf: LazyFrame) -> PolarsResult<DataFrame> {
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

type RawMeta = (bool, String);

#[instrument]
pub fn preproc_checked<'a>(
    boxed_raw_taxi_data: &'a [(&'a RawData<'a>, RawMeta)],
) -> Vec<NamedData<'a>> {
    let scanned_weather_data = scan_csv(Path::new(WEATHER_RAW))
        .unwrap_or_else(|e| panic!("Failed to parse weather data: {e}"));
    boxed_raw_taxi_data
        .par_iter()
        .map(|(data, (exists, path))| {
            if !exists {
                info!("Couldn't find {path:?} ... running full scan");
                preproc(data, &scanned_weather_data)
            } else {
                info!("Found {path:?} ... loading from cache");
                load_csv(path, data.name)
            }
        })
        .collect()
}

#[instrument]
fn preproc<'a>(raw_taxi_data: &RawData<'a>, scanned_weather_data: &DataFrame) -> NamedData<'a> {
    let RawData { name, ent } = raw_taxi_data;
    let NamedData { name, df } = LazyFrame::scan_parquet(ent, ScanArgsParquet::default())
        .and_then(clean)
        .map(|df| NamedData { name, df })
        .unwrap();

    let mut joined_taxi_weather: NamedData = df
        .join(
            scanned_weather_data,
            ["time"],
            ["time_ns"],
            JoinArgs::new(JoinType::AsOf(AsOfOptions::default())),
            None,
        )
        .map(|v| NamedData::new(name, v.drop_many(["time", "time_ns"])))
        .unwrap_or_else(|e| panic!("Failed to join taxi and weather data; {e}"));

    save_csv(&mut joined_taxi_weather);
    joined_taxi_weather
}
