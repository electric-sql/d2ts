import { createCanvas } from 'canvas'
import { Chart, ChartConfiguration } from 'chart.js'
import { registerables } from 'chart.js'

// Register the components we need
Chart.register(...registerables)

interface BenchmarkResult {
  name: string
  firstRunTime: number
  incrementalTime: number
  opsPerSec: number
}

interface SizeResults {
  initialSize: number
  results: BenchmarkResult[]
}

export function generateGraph(results: SizeResults[]) {
  // Create a canvas with a reasonable size
  const canvas = createCanvas(1200, 800)
  const ctx = canvas.getContext('2d') as unknown as CanvasRenderingContext2D

  // Set white background
  ctx.fillStyle = 'white'
  ctx.fillRect(0, 0, canvas.width, canvas.height)

  // Prepare data for the chart
  const datasets = new Map<string, { data: number[] }>()
  const labels = results.map(r => r.initialSize.toString())

  // Process results to create datasets
  results.forEach(sizeResult => {
    sizeResult.results.forEach(result => {
      if (!datasets.has(result.name)) {
        datasets.set(result.name, { data: [] })
      }
      datasets.get(result.name)!.data.push(result.incrementalTime)
    })
  })

  // Create chart configuration
  const config: ChartConfiguration<'line'> = {
    type: 'line',
    data: {
      labels,
      datasets: Array.from(datasets.entries()).map(([name, data], index) => ({
        label: name,
        data: data.data,
        borderColor: `hsl(${(index * 360) / datasets.size}, 70%, 50%)`,
        backgroundColor: `hsla(${(index * 360) / datasets.size}, 70%, 50%, 0.1)`,
        tension: 0,
        fill: false,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          type: 'logarithmic',
          min: 0.001, // Set a minimum value to avoid log(0)
          title: {
            display: true,
            text: 'Time (ms)',
          },
        },
        x: {
          title: {
            display: true,
            text: 'Initial Size',
          },
        },
      },
      plugins: {
        title: {
          display: true,
          text: 'Benchmark Results by Initial Size',
          font: {
            size: 16,
          },
        },
        legend: {
          position: 'top',
        },
      },
    },
  }

  // Create and render the chart
  new Chart(ctx, config)

  // Save the canvas to a file
  const fs = require('fs')
  const buffer = canvas.toBuffer('image/png')
  fs.writeFileSync('benchmark-results.png', buffer)
} 