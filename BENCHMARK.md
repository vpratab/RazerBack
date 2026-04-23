# Benchmark

- Interpreter: `C:\fx_data\pipeline-venv\Scripts\python.exe`
- Rust extension available in current environment: `True`

## Hot Path Microbenchmarks
- Hawkes Python fallback: `4.0040s`
- Hawkes Rust-enabled: `1.6044s`
- Hawkes speedup: `2.50x`
- Trade-path Python fallback: `4.1412s`
- Trade-path Rust-enabled: `0.7032s`
- Trade-path speedup: `5.89x`

## Full Run
- Full backtest Python fallback: `2.82s`
- Full backtest Rust-enabled: `2.79s`
- Full backtest speedup: `1.01x`

## Notes
- The trade-path benchmark is the cleanest measure of the Rust hot path because it isolates repeated exit simulation work.
- The full-run benchmark reflects current sample data and the public runtime structure, so end-to-end speedup is usually smaller than the microbenchmark speedup.
- `rolling_gmm_nodes` is currently routed through the Rust extension with a parity-safe bridge to the Python reference logic.
