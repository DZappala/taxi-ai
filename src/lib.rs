use std::path::Path;

use charming::WasmRenderer;
use chart::chart;
use clean::{RawData, preproc_checked};
use dioxus::prelude::*;
use itertools::Itertools;

pub mod chart;
pub mod clean;

pub const JAN_RAW: &str = "db/raw/yellow_tripdata_2020-01.parquet";
pub const MAR_RAW: &str = "db/raw/yellow_tripdata_2020-03.parquet";
pub const MAY_RAW: &str = "db/raw/yellow_tripdata_2020-05.parquet";

#[component]
pub fn App() -> Element {
    let renderer = use_signal(|| WasmRenderer::new(2560, 1440));
    use_effect(move || {
        let raw_taxi_data = [
            RawData::new("Jan", JAN_RAW),
            RawData::new("Mar", MAR_RAW),
            RawData::new("May", MAY_RAW),
        ];
        let zip = &raw_taxi_data
            .iter()
            .zip(raw_taxi_data.iter().map(|RawData { name, ent: _ }| {
                let path = format!("db/record/taxi_weather-{name}.csv");
                (Path::new(&path).exists(), path)
            }))
            .collect_vec();
        let dfs = preproc_checked(zip);

        dfs.iter().for_each(|df| {
            let chart = chart(df.df());
            renderer.read_unchecked().render(df.name(), &chart).unwrap();
        });
    });

    rsx!(
        div {
            style: "text-align: center; display: flex; flex-direction: column; gap: 2px;",
            h1 {"🔥 Heatmap Inspector"}
        }
        div {
            style: "width: 100%; text-align: center;",
            div {id: "Jan", style: "display: inline-block;" }
        }
        div {
            style: "width: 100%; text-align: center;",
            div {id: "Mar", style: "display: inline-block;" }
        }
        div {
            style: "width: 100%; text-align: center;",
            div {id: "May", style: "display: inline-block;" }
        }

    )
}
