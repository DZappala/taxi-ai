use charming::{
    Chart, HtmlRenderer,
    component::{Axis, Grid, Legend, VisualMap},
    datatype::DataPoint,
    df,
    element::{AxisType, Emphasis, ItemStyle, Label, Orient, SplitArea, Tooltip},
    series::Heatmap,
};

use itertools::Itertools;
use polars::prelude::{cov::pearson_corr, *};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::{instrument, trace};

#[instrument]
pub fn chart(df: &DataFrame) -> Chart {
    trace!("chart()");
    trace!("Transforming dataframe");
    let df = df
        .par_materialized_column_iter()
        .map(|c| {
            df![df
                .iter()
                .map(|d| {
                    pearson_corr(
                        c.cast(&DataType::Float64).unwrap().f64().unwrap(),
                        d.cast(&DataType::Float64).unwrap().f64().unwrap(),
                    )
                    .unwrap()
                })
                .collect_vec()]
            .into_par_iter()
            .map(|d| match d {
                DataPoint::Value(val) => DataPoint::Value(val),
                DataPoint::Item(item) => DataPoint::Item(item.name(c.name().to_string())),
            })
            .collect::<Vec<DataPoint>>()
        })
        .collect();

    trace!("Creating chart");
    Chart::new()
        .tooltip(Tooltip::new().position("top"))
        .grid(Grid::new().height("50%").top("10%"))
        .x_axis(
            Axis::new()
                .type_(AxisType::Value)
                .split_area(SplitArea::new().show(true)),
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .split_area(SplitArea::new().show(true)),
        )
        .visual_map(
            VisualMap::new()
                .min(0)
                .max(10)
                .calculable(true)
                .orient(Orient::Horizontal)
                .left("center")
                .bottom("15%"),
        )
        .series(
            Heatmap::new()
                .name("Feature Correlation")
                .label(Label::new().show(true))
                .emphasis(
                    Emphasis::new().item_style(
                        ItemStyle::new()
                            .shadow_blur(10)
                            .shadow_color("rgba(0, 0, 0, 0.5)"),
                    ),
                )
                .data(df),
        )
}

pub fn render(chart: Chart, name: &'static str) {
    let mut renderer = HtmlRenderer::new(name, 1000, 1000);
    renderer.save(&chart, "out/charts.html").unwrap();
}
