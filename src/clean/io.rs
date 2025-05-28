use std::{
    fs::{File, exists, remove_file},
    path::Path,
};

use polars::prelude::*;
use tracing::{info, instrument, trace};

pub const JAN_RAW: &str = "db/raw/yellow_tripdata_2020-01.parquet";
pub const MAR_RAW: &str = "db/raw/yellow_tripdata_2020-03.parquet";
pub const MAY_RAW: &str = "db/raw/yellow_tripdata_2020-05.parquet";
pub const WEATHER_RAW: &str = "db/raw/weather.csv";
pub const COLS_TO_REMOVE: [&str; 51] = [
    "pickup_hour_cos",
    "pickup_hour_sin",
    "pickup_day_cos",
    "pickup_day_sin",
    "passenger_count",
    "tolls_amount",
    "relative_humidity_2m_pct",
    "apparent_temperature_C",
    "precipitation_mm",
    "soil_temperature_0_to_7cm_C",
    "soil_moisture_0_to_7cm_m3pm3",
    "cloud_cover_pct",
    "surface_pressure_hPa",
    "wind_speed_10m_kmph",
    "wind_direction_10m_deg",
    "wind_gusts_10m_kmph",
    "time_right",
    "snowfall_cm",
    "snow_depth_m",
    "is_day_",
    "sunshine_duration_s",
    "shortwave_radiation_Wpm2",
    "diffuse_radiation_Wpm2",
    "global_tilted_irradiance_Wpm2",
    "shortwave_radiation_instant_Wpm2",
    "diffuse_radiation_instant_Wpm2",
    "terrestrial_radiation_instant_Wpm2",
    "direct_normal_irradiance_instant_Wpm2",
    "direct_radiation_instant_Wpm2",
    "terrestrial_radiation_Wpm2",
    "direct_normal_irradiance_Wpm2",
    "direct_radiation_Wpm2",
    "dew_point_2m_C",
    "soil_temperature_7_to_28cm_C",
    "soil_temperature_28_to_100cm_C",
    "soil_temperature_100_to_255cm_C",
    "soil_moisture_7_to_28cm_m3pm3",
    "soil_moisture_28_to_100cm_m3pm3",
    "soil_moisture_100_to_255cm_m3pm3",
    "vapour_pressure_deficit_kPa",
    "et0_fao_evapotranspiration_mm",
    "cloud_cover_high_pct",
    "cloud_cover_mid_pct",
    "cloud_cover_low_pct",
    "pressure_msl_hPa",
    "weather_code_wmo code",
    "wind_speed_100m_kmph",
    "wind_direction_100m_deg",
    "total_column_integrated_water_vapour_kgpm2",
    "wet_bulb_temperature_2m_C",
    "boundary_layer_height_m",
];

#[instrument]
pub fn scan_csv(path: &Path) -> PolarsResult<DataFrame> {
    trace!("scan_csv()");
    trace!("Reading csv file {:?}", path.to_str());
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

#[derive(Debug)]
pub struct NamedDataFrame<'a> {
    pub(super) name: &'a str,
    pub(super) df: DataFrame,
}

impl<'a> NamedDataFrame<'a> {
    #[instrument]
    pub(super) fn new(name: &'a str, df: DataFrame) -> Self {
        trace!("new()");
        trace!("New NamedDataFrame {name:?}");
        Self { name, df }
    }

    #[instrument]
    pub fn df(&self) -> &DataFrame {
        trace!("df()");
        trace!("Get df {:?}", self.name);
        &self.df
    }
}

#[derive(Debug)]
pub(super) struct RawDataEntry<'a> {
    pub(super) name: &'a str,
    pub(super) ent: &'static str,
}

impl<'a> RawDataEntry<'a> {
    #[instrument]
    pub(super) fn new(name: &'a str, ent: &'static str) -> Self {
        trace!("new()");
        trace!("New RawDataEntry {name:?}");
        Self { name, ent }
    }
}

#[instrument]
pub fn save_csv(
    &mut NamedDataFrame {
        ref mut name,
        ref mut df,
    }: &mut NamedDataFrame<'_>,
) {
    trace!("save_csv()");
    let path = format!("db/record/taxi_weather-{name}.csv");
    trace!("Saving csv file to {path:?}");
    if exists(&path).unwrap() {
        info!("Removing csv file. Already exists.");
        remove_file(&path).unwrap();
    }

    info!("Creating csv file {path:?}");
    let mut file = File::create(path).unwrap();
    CsvWriter::new(&mut file).finish(df).unwrap();
}
