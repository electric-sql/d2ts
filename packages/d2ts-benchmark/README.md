# D2TS Benchmark

Performance benchmarks for D2TS operators and functions.

## Available Benchmarks

### General Benchmarks
```bash
# Run general benchmarks
npm run bench
```

### Join Operator Benchmarks
```bash
# Run join operator benchmarks with default settings
npm run bench:join

# Run with custom settings
npm run bench:join -- --initialSize=2000 --incrementalSize=200 --incrementalRuns=5
```

The join benchmark compares the performance of:
- In-memory join
- SQLite join
- In-memory joinAll (with 2 streams)
- SQLite joinAll (with 2 streams)

This helps verify that the joinAll operator performs similarly to the standard join operator when used with the same number of streams. 