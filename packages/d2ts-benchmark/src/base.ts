import { table } from 'table'
import type { TableUserConfig } from 'table'

export interface Benchmark<T> {
  name: string
  setup: () => T
  firstRun: (ctx: T) => void
  incrementalRun: (ctx: T, i: number) => void
  teardown?: (ctx: T) => void
}

interface RunResult {
  startupTime: number
  firstRunTime: number
  incrementalTimes: number[]
  teardownTime: number
}

export interface SuiteOptions {
  name: string
  totalRuns?: number
  incrementalRuns?: number
  warmupRuns?: number
  maxTimeDoingIncrementalRuns?: number
}

export class Suite {
  name: string
  totalRuns: number
  incrementalRuns: number
  warmupRuns: number
  private benchmarks: Benchmark<unknown>[] = []
  private results: RunResult[] = []
  private maxTimeDoingIncrementalRuns?: number

  constructor(options: SuiteOptions) {
    this.name = options.name
    this.totalRuns = options.totalRuns ?? 5
    this.incrementalRuns = options.incrementalRuns ?? 100
    this.warmupRuns = options.warmupRuns ?? 1
    this.maxTimeDoingIncrementalRuns = options.maxTimeDoingIncrementalRuns
  }

  add<T>(benchmark: Benchmark<T>) {
    this.benchmarks.push(benchmark as Benchmark<unknown>)
  }

  runTest(benchmark: Benchmark<unknown>): RunResult {
    const result: RunResult = {
      startupTime: 0,
      firstRunTime: 0,
      incrementalTimes: new Array(this.incrementalRuns).fill(0),
      teardownTime: 0,
    }

    let start = performance.now()
    const ctx = benchmark.setup()
    result.startupTime = performance.now() - start

    start = performance.now()
    benchmark.firstRun(ctx)
    result.firstRunTime = performance.now() - start

    const startedAt = performance.now()
    for (let i = 0; i < this.incrementalRuns; i++) {
      start = performance.now()
      benchmark.incrementalRun(ctx, i)
      result.incrementalTimes[i] = performance.now() - start
      if (
        this.maxTimeDoingIncrementalRuns &&
        performance.now() - startedAt > this.maxTimeDoingIncrementalRuns
      ) {
        // truncate the array to the current index
        result.incrementalTimes = result.incrementalTimes.slice(0, i)
        break
      }
    }

    if (benchmark.teardown) {
      start = performance.now()
      benchmark.teardown(ctx)
      result.teardownTime = performance.now() - start
    }

    return result
  }

  run() {
    for (const benchmark of this.benchmarks) {
      for (let i = 0; i < this.warmupRuns; i++) {
        this.runTest(benchmark)
      }
      for (let i = 0; i < this.totalRuns; i++) {
        this.results.push(this.runTest(benchmark))
      }
    }
  }

  printResults() {
    // Group results by benchmark
    const resultsByBenchmark = new Map<string, RunResult[]>()
    let i = 0
    for (const benchmark of this.benchmarks) {
      resultsByBenchmark.set(
        benchmark.name,
        this.results.slice(i * this.totalRuns, (i + 1) * this.totalRuns),
      )
      i++
    }

    // Calculate data for the table
    const headers = [
      'Benchmark',
      // 'Startup (ms)',
      'First Run (ms)',
      'Incremental (ms)',
      'ops/sec',
      // 'Teardown (ms)',
    ]
    const rows = Array.from(resultsByBenchmark.entries()).map(
      ([name, results]) => {
        const avgIncrementalMs = results.map(
          (r) =>
            r.incrementalTimes.reduce((a, b) => a + b, 0) /
            r.incrementalTimes.length,
        )

        return [
          name,
          // formatAverage(results.map((r) => r.startupTime)),
          formatAverage(results.map((r) => r.firstRunTime)),
          formatAverage(avgIncrementalMs),
          formatOpsPerSec(avgIncrementalMs.map((ms) => 1000 / ms)),
          // formatAverage(results.map((r) => r.teardownTime)),
        ]
      },
    )

    const data = [headers, ...rows]

    const config: TableUserConfig = {
      columns: {
        0: { alignment: 'left' },
        1: { alignment: 'right' },
        2: { alignment: 'right' },
        3: { alignment: 'right' },
        4: { alignment: 'right' },
        5: { alignment: 'right' },
      },
      border: {
        topBody: `─`,
        topJoin: `┬`,
        topLeft: `┌`,
        topRight: `┐`,

        bottomBody: `─`,
        bottomJoin: `┴`,
        bottomLeft: `└`,
        bottomRight: `┘`,

        bodyLeft: `│`,
        bodyRight: `│`,
        bodyJoin: `│`,

        joinBody: `─`,
        joinLeft: `├`,
        joinRight: `┤`,
        joinJoin: `┼`,
      },
    }

    console.log(`\nResults for ${this.name}:`)
    console.log(table(data, config))
  }

  getResults(): { name: string; firstRunTime: number; incrementalTime: number; opsPerSec: number }[] {
    const resultsByBenchmark = new Map<string, RunResult[]>()
    let i = 0
    for (const benchmark of this.benchmarks) {
      resultsByBenchmark.set(
        benchmark.name,
        this.results.slice(i * this.totalRuns, (i + 1) * this.totalRuns),
      )
      i++
    }

    return Array.from(resultsByBenchmark.entries()).map(([name, results]) => {
      const avgIncrementalMs = results.map(
        (r) =>
          r.incrementalTimes.reduce((a, b) => a + b, 0) /
          r.incrementalTimes.length,
      )
      const avgFirstRunMs = results.map((r) => r.firstRunTime)
      const avgOpsPerSec = avgIncrementalMs.map((ms) => 1000 / ms)

      return {
        name,
        firstRunTime: avgFirstRunMs.reduce((a, b) => a + b, 0) / avgFirstRunMs.length,
        incrementalTime: avgIncrementalMs.reduce((a, b) => a + b, 0) / avgIncrementalMs.length,
        opsPerSec: avgOpsPerSec.reduce((a, b) => a + b, 0) / avgOpsPerSec.length,
      }
    })
  }
}

function formatAverage(numbers: number[]): string {
  const avg = numbers.reduce((a, b) => a + b, 0) / numbers.length
  return avg.toFixed(4)
}

function formatOpsPerSec(numbers: number[]): string {
  const avg = Math.round(numbers.reduce((a, b) => a + b, 0) / numbers.length)
  return `${avg.toLocaleString('en-US')}`
}
