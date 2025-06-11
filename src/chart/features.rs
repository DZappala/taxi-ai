use charming::{
    Chart, HtmlRenderer,
    component::{Axis, Grid, VisualMap},
    datatype::DataPoint,
    df as charming_df,
    element::{AxisType, Emphasis, ItemStyle, Label, Orient, SplitArea, Tooltip},
    series::Heatmap,
};

use itertools::Itertools;
use polars::prelude::{cov::pearson_corr, *};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use tracing::{instrument, trace};

#[instrument]
pub fn chart(data: &DataFrame) -> Chart {
    trace!("chart()");
    trace!("Transforming dataframe");

    let cols: Vec<_> = data
        .get_columns()
        .par_iter()
        .map(|s| s.name().to_string())
        .collect();

    let out_data: Vec<Vec<DataPoint>> = cols
        .par_iter()
        .enumerate()
        .flat_map(|(i, col_i)| {
            cols.par_iter().enumerate().filter_map(move |(j, col_j)| {
                let corr = pearson_corr(
                    data.column(col_i).ok()?.f64().ok()?,
                    data.column(col_j).ok()?.f64().ok()?,
                )
                .unwrap_or(0.0);

                Some(vec![
                    DataPoint::from(i as i32),
                    DataPoint::from(j as i32),
                    DataPoint::from(corr),
                ])
            })
        })
        .collect();

    Chart::new()
        .tooltip(Tooltip::new().position("top"))
        .grid(Grid::new().height("50%").top("10%"))
        .x_axis(
            Axis::new()
                .type_(AxisType::Category)
                .data(cols.clone())
                .split_area(SplitArea::new().show(true)),
        )
        .y_axis(
            Axis::new()
                .type_(AxisType::Category)
                .data(cols)
                .split_area(SplitArea::new().show(true)),
        )
        .visual_map(
            VisualMap::new()
                .min(-1.0)
                .max(1.0)
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
                .data(out_data),
        )
}

pub fn render(chart: Chart, name: &'static str) {
    let mut renderer = HtmlRenderer::new(name, 1000, 1000);
    renderer.save(&chart, "out/charts.html").unwrap();
}
