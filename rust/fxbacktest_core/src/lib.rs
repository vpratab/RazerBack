use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::Serialize;

#[derive(Serialize)]
struct PartialFill {
    bar_offset: usize,
    price: f64,
    fraction: f64,
    pnl_delta: f64,
    event: String,
}

#[pyfunction]
fn hawkes_intensity(shocks: Vec<f64>, alpha: f64) -> Vec<f64> {
    let decay = (-alpha).exp();
    let mut current = 0.0_f64;
    let mut out = Vec::with_capacity(shocks.len());
    for shock in shocks {
        current = current * decay + shock;
        out.push(current);
    }
    out
}

#[pyfunction]
fn simulate_trade_path_rust(
    _open_bid: Vec<f64>,
    high_bid: Vec<f64>,
    low_bid: Vec<f64>,
    close_bid: Vec<f64>,
    entry_price: f64,
    stop_loss: f64,
    ladder_pips: Vec<f64>,
    ladder_fractions: Vec<f64>,
    trail_stop_pips: f64,
    ttl_bars: usize,
    side: String,
) -> PyResult<(f64, usize, String)> {
    if close_bid.is_empty() {
        return Ok((0.0, 0, "[]".to_string()));
    }
    let side_lc = side.to_lowercase();
    let end_idx = usize::min(close_bid.len().saturating_sub(1), ttl_bars);
    let mut remaining = 1.0_f64;
    let mut total_pnl_delta = 0.0_f64;
    let mut trailing_active = false;
    let mut trailing_stop = stop_loss;
    let mut highest_price = entry_price;
    let mut lowest_price = entry_price;
    let mut exit_idx = end_idx;
    let mut exit_price = close_bid[end_idx];
    let mut hit_levels = vec![false; ladder_pips.len()];
    let mut partials: Vec<PartialFill> = Vec::new();

    for bar_idx in 0..=end_idx {
        if side_lc == "long" {
            let bar_high = high_bid[bar_idx];
            let bar_low = low_bid[bar_idx];
            for (level_idx, (delta, frac)) in ladder_pips.iter().zip(ladder_fractions.iter()).enumerate() {
                if hit_levels[level_idx] || remaining <= 0.0 {
                    continue;
                }
                let target_price = entry_price + delta;
                if bar_high >= target_price {
                    let fill_fraction = remaining.min(*frac);
                    let pnl_delta = target_price - entry_price;
                    total_pnl_delta += pnl_delta * fill_fraction;
                    remaining -= fill_fraction;
                    hit_levels[level_idx] = true;
                    partials.push(PartialFill {
                        bar_offset: bar_idx,
                        price: target_price,
                        fraction: fill_fraction,
                        pnl_delta,
                        event: "target".to_string(),
                    });
                    highest_price = highest_price.max(bar_high);
                    if !trailing_active {
                        trailing_active = true;
                        trailing_stop = target_price - trail_stop_pips;
                    }
                }
            }

            if trailing_active && remaining > 0.0 {
                highest_price = highest_price.max(bar_high);
                trailing_stop = trailing_stop.max(highest_price - trail_stop_pips);
            }

            let active_stop = if trailing_active { trailing_stop } else { stop_loss };
            if remaining > 0.0 && bar_low <= active_stop {
                let pnl_delta = active_stop - entry_price;
                total_pnl_delta += pnl_delta * remaining;
                exit_idx = bar_idx;
                exit_price = active_stop;
                partials.push(PartialFill {
                    bar_offset: bar_idx,
                    price: active_stop,
                    fraction: remaining,
                    pnl_delta,
                    event: "stop".to_string(),
                });
                remaining = 0.0;
                break;
            }
        } else {
            let bar_high = high_bid[bar_idx];
            let bar_low = low_bid[bar_idx];
            for (level_idx, (delta, frac)) in ladder_pips.iter().zip(ladder_fractions.iter()).enumerate() {
                if hit_levels[level_idx] || remaining <= 0.0 {
                    continue;
                }
                let target_price = entry_price - delta;
                if bar_low <= target_price {
                    let fill_fraction = remaining.min(*frac);
                    let pnl_delta = entry_price - target_price;
                    total_pnl_delta += pnl_delta * fill_fraction;
                    remaining -= fill_fraction;
                    hit_levels[level_idx] = true;
                    partials.push(PartialFill {
                        bar_offset: bar_idx,
                        price: target_price,
                        fraction: fill_fraction,
                        pnl_delta,
                        event: "target".to_string(),
                    });
                    lowest_price = lowest_price.min(bar_low);
                    if !trailing_active {
                        trailing_active = true;
                        trailing_stop = target_price + trail_stop_pips;
                    }
                }
            }

            if trailing_active && remaining > 0.0 {
                lowest_price = lowest_price.min(bar_low);
                trailing_stop = trailing_stop.min(lowest_price + trail_stop_pips);
            }

            let active_stop = if trailing_active { trailing_stop } else { stop_loss };
            if remaining > 0.0 && bar_high >= active_stop {
                let pnl_delta = entry_price - active_stop;
                total_pnl_delta += pnl_delta * remaining;
                exit_idx = bar_idx;
                exit_price = active_stop;
                partials.push(PartialFill {
                    bar_offset: bar_idx,
                    price: active_stop,
                    fraction: remaining,
                    pnl_delta,
                    event: "stop".to_string(),
                });
                remaining = 0.0;
                break;
            }
        }
    }

    if remaining > 0.0 {
        let ttl_price = close_bid[end_idx];
        let pnl_delta = if side_lc == "long" {
            ttl_price - entry_price
        } else {
            entry_price - ttl_price
        };
        total_pnl_delta += pnl_delta * remaining;
        exit_idx = end_idx;
        exit_price = ttl_price;
        partials.push(PartialFill {
            bar_offset: end_idx,
            price: ttl_price,
            fraction: remaining,
            pnl_delta,
            event: "ttl".to_string(),
        });
    }

    if partials.is_empty() {
        partials.push(PartialFill {
            bar_offset: exit_idx,
            price: exit_price,
            fraction: 1.0,
            pnl_delta: total_pnl_delta,
            event: "ttl".to_string(),
        });
    }

    let partial_json = serde_json::to_string(&partials)
        .map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))?;
    Ok((total_pnl_delta, exit_idx, partial_json))
}

#[pyfunction]
fn rolling_gmm_nodes(
    prices: Vec<f64>,
    timestamps: Vec<i64>,
    lookback_hours: usize,
    components: usize,
    refit_hours: usize,
) -> PyResult<Vec<f64>> {
    if prices.is_empty() || timestamps.is_empty() || prices.len() != timestamps.len() {
        return Ok(Vec::new());
    }

    Python::with_gil(|py| -> PyResult<Vec<f64>> {
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("unit", "ms")?;
        kwargs.set_item("utc", true)?;
        let pandas = PyModule::import_bound(py, "pandas")?;
        let timestamps_dt = pandas
            .getattr("to_datetime")?
            .call((timestamps,), Some(&kwargs))?;
        let module = PyModule::import_bound(py, "native_acceleration")?;
        let function = module.getattr("rolling_gmm_nodes_reference")?;
        let result = function.call1((timestamps_dt, prices, components, lookback_hours, refit_hours))?;
        result.call_method0("tolist")?.extract()
    })
}

#[pymodule]
fn fxbacktest_core(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(hawkes_intensity, module)?)?;
    module.add_function(wrap_pyfunction!(simulate_trade_path_rust, module)?)?;
    module.add_function(wrap_pyfunction!(rolling_gmm_nodes, module)?)?;
    Ok(())
}
