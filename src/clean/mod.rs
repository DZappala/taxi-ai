use std::path::Path;

use polars::prelude::*;

const JAN_RAW: &str = "raw/yellow_tripdata_2020-01.parquet";
const MAR_RAW: &str = "raw/yellow_tripdata_2020-03.parquet";
const MAY_RAW: &str = "raw/yellow_tripdata_2020-05.parquet";
const WEATHER_RAW: &str = "raw/weather.csv";

fn scan_parquet(path: &Path) -> PolarsResult<DataFrame> {
    let df = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?
        .select([
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
        .filter(col("total_seconds").gt(0))
        .filter(col("total_seconds").lt(36000))
        .collect()?;
    Ok(df)
}

fn scan_csv(path: &Path) -> PolarsResult<DataFrame> {
    LazyCsvReader::new(path)
        .with_has_header(true)
        .with_try_parse_dates(true)
        .with_infer_schema_length(Some(10000))
        .finish()?
        .select([
            all(),
            col("time")
                .dt()
                .cast_time_unit(TimeUnit::Nanoseconds)
                .alias("time_ns"),
        ])
        .collect()
}

struct NamedDataFrame<'a>(&'a str, DataFrame);
struct RawDataEntry<'a>(&'a str, &'static str);

fn main() {
    let raw_taxi_data: Vec<RawDataEntry> = vec![
        RawDataEntry("Jan", JAN_RAW),
        RawDataEntry("Mar", MAR_RAW),
        RawDataEntry("May", MAY_RAW),
    ];

    let scanned_taxi_data: Vec<NamedDataFrame> = raw_taxi_data
        .into_iter()
        .filter_map(|RawDataEntry(_0, _1)| {
            scan_parquet(Path::new(_1))
                .ok()
                .map(|x: DataFrame| NamedDataFrame(_0, x))
        })
        .collect::<Vec<NamedDataFrame>>();

    assert!(scanned_taxi_data.len() == 3);

    let scanned_weather_data = match scan_csv(Path::new(WEATHER_RAW)) {
        Ok(x) => x,
        Err(err) => panic!("Failed to parse weather data: {err}"),
    };

    let mut joined_taxi_weather: Vec<(&str, DataFrame)> = scanned_taxi_data
        .iter()
        .map(|NamedDataFrame(_0, _1)| {
            match _1.join(
                &scanned_weather_data,
                ["time"],
                ["time_ns"],
                JoinArgs::new(JoinType::AsOf(AsOfOptions::default())),
                None,
            ) {
                Ok(val) => (*_0, val.drop_many(["time", "time_ns"])),
                Err(err) => panic!("Failed to join taxi and weather data; {err}"),
            }
        })
        .collect();

    joined_taxi_weather.iter_mut().for_each(|(_0, _1)| {
        println!(
            "{}",
            _1.column("total_seconds").unwrap().get(0).unwrap().dtype()
        );
        let mut file =
            std::fs::File::create(format!("../project2/db/record/taxi_weather-{_0}.csv")).unwrap();
        CsvWriter::new(&mut file).finish(_1).unwrap();
    });
}
