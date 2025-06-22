import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { reduce } from '../../src/operators/reduce.js'
import { output } from '../../src/operators/output.js'

describe('Operators', () => {
  describe('Reduce operation', () => {
    test('basic reduce operation', () => {
      const graph = new D2()
      const input = graph.newInput<[string, number]>()
      const messages: MultiSet<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [['a', 1], 2],
          [['a', 2], 1],
          [['a', 3], 1],
          [['b', 4], 1],
        ]),
      )
      input.sendData(new MultiSet([[['b', 5], 1]]))
      graph.run()

      const data = messages.map((m) => m.getInner())

      expect(data).toEqual([
        [
          [['a', 7], 1],
          [['b', 9], 1],
        ],
      ])
    })

    test('reduce with negative multiplicities', () => {
      const graph = new D2()
      const input = graph.newInput<[string, number]>()
      const messages: MultiSet<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [['a', 1], -1],
          [['a', 2], 2],
          [['b', 3], -2],
        ]),
      )
      graph.run()

      const data = messages.map((m) => m.getInner())

      expect(data).toEqual([
        [
          [['a', 3], 1],
          [['b', -6], 1],
        ],
      ])
    })

    test('multiple incremental updates to same key', () => {
      const graph = new D2()
      const input = graph.newInput<[string, number]>()
      const messages: MultiSet<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // First update: a=1, b=2
      input.sendData(
        new MultiSet([
          [['a', 1], 1],
          [['b', 2], 1],
        ]),
      )
      graph.run()

      // Second update: add more to a, modify b
      input.sendData(
        new MultiSet([
          [['a', 3], 1],
          [['b', 4], 1],
        ]),
      )
      graph.run()

      // Third update: remove some from a
      input.sendData(new MultiSet([[['a', 1], -1]]))
      graph.run()

      const data = messages.map((m) => m.getInner())

      expect(data).toEqual([
        // First update: a=1, b=2
        [
          [['a', 1], 1],
          [['b', 2], 1],
        ],
        // Second update: old values removed, new values added
        [
          [['a', 1], -1], // Remove old sum for a
          [['a', 4], 1], // Add new sum for a (1+3)
          [['b', 2], -1], // Remove old sum for b
          [['b', 6], 1], // Add new sum for b (2+4)
        ],
        // Third update: remove a=1, so new sum is just 3
        [
          [['a', 4], -1], // Remove old sum for a
          [['a', 3], 1], // Add new sum for a (just 3 now)
        ],
      ])
    })

    test('updates that cancel out completely', () => {
      const graph = new D2()
      const input = graph.newInput<[string, number]>()
      const messages: MultiSet<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // First update: add values
      input.sendData(
        new MultiSet([
          [['a', 5], 1],
          [['a', 3], 1],
          [['b', 10], 1],
        ]),
      )
      graph.run()

      // Second update: cancel out all values for 'a'
      input.sendData(
        new MultiSet([
          [['a', 5], -1],
          [['a', 3], -1],
        ]),
      )
      graph.run()

      const data = messages.map((m) => m.getInner())

      expect(data).toEqual([
        // First update: a=8, b=10
        [
          [['a', 8], 1],
          [['b', 10], 1],
        ],
        // Second update: remove old sum, add new sum (which is 0)
        [
          [['a', 8], -1], // Remove old sum for a
          [['a', 0], 1], // Add new sum for a (which is 0)
        ],
      ])
    })

    test('mixed positive and negative updates', () => {
      const graph = new D2()
      const input = graph.newInput<[string, number]>()
      const messages: MultiSet<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // First update: establish initial state
      input.sendData(
        new MultiSet([
          [['a', 10], 1],
          [['a', 5], 2],
          [['b', 20], 1],
        ]),
      )
      graph.run()

      // Second update: mix of adds and removes
      input.sendData(
        new MultiSet([
          [['a', 10], -1], // Remove one 10
          [['a', 2], 1], // Add a 2
          [['b', 20], -1], // Remove the 20
          [['b', 15], 1], // Add a 15
          [['c', 100], 1], // Add new key
        ]),
      )
      graph.run()

      const data = messages.map((m) => m.getInner())

      expect(data).toEqual([
        // First update: a=20 (10+5+5), b=20
        [
          [['a', 20], 1],
          [['b', 20], 1],
        ],
        // Second update: a=12 (5+5+2), b=15, c=100
        [
          [['a', 20], -1], // Remove old sum for a
          [['a', 12], 1], // Add new sum for a
          [['b', 20], -1], // Remove old sum for b
          [['b', 15], 1], // Add new sum for b
          [['c', 100], 1], // Add new key c
        ],
      ])
    })

    test('complex aggregation with multiple updates', () => {
      const graph = new D2()
      const input = graph.newInput<[string, { value: number; count: number }]>()
      const messages: MultiSet<[string, { avg: number; total: number }]>[] = []

      input.pipe(
        reduce((vals) => {
          let totalSum = 0
          let totalCount = 0
          for (const [val, diff] of vals) {
            totalSum += val.value * val.count * diff
            totalCount += val.count * diff
          }
          const avg = totalCount > 0 ? totalSum / totalCount : 0
          return [[{ avg, total: totalSum }, 1]]
        }),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // First batch: group 'a' has values
      input.sendData(
        new MultiSet([
          [['a', { value: 10, count: 2 }], 1], // 2 values of 10
          [['a', { value: 20, count: 1 }], 1], // 1 value of 20
        ]),
      )
      graph.run()

      // Second batch: add more to 'a' and start 'b'
      input.sendData(
        new MultiSet([
          [['a', { value: 30, count: 1 }], 1], // 1 value of 30
          [['b', { value: 50, count: 3 }], 1], // 3 values of 50
        ]),
      )
      graph.run()

      // Third batch: remove some from 'a'
      input.sendData(
        new MultiSet([
          [['a', { value: 10, count: 2 }], -1], // Remove the 2 values of 10
        ]),
      )
      graph.run()

      const data = messages.map((m) => m.getInner())

      expect(data).toEqual([
        // First update: a avg=(10*2+20*1)/(2+1)=40/3≈13.33, total=40
        [[['a', { avg: 40 / 3, total: 40 }], 1]],
        // Second update:
        // a avg=(10*2+20*1+30*1)/(2+1+1)=70/4=17.5, total=70
        // b avg=50, total=150
        [
          [['a', { avg: 40 / 3, total: 40 }], -1], // Remove old
          [['a', { avg: 17.5, total: 70 }], 1], // Add new
          [['b', { avg: 50, total: 150 }], 1], // New key
        ],
        // Third update: a avg=(20*1+30*1)/(1+1)=50/2=25, total=50
        [
          [['a', { avg: 17.5, total: 70 }], -1], // Remove old
          [['a', { avg: 25, total: 50 }], 1], // Add new
        ],
      ])
    })

    test('updates with zero-multiplicity results', () => {
      const graph = new D2()
      const input = graph.newInput<[string, number]>()
      const messages: MultiSet<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          // Only return non-zero sums
          return sum !== 0 ? [[sum, 1]] : []
        }),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // First update: establish values
      input.sendData(
        new MultiSet([
          [['a', 5], 1],
          [['a', -3], 1],
          [['b', 10], 1],
        ]),
      )
      graph.run()

      // Second update: make 'a' sum to zero
      input.sendData(new MultiSet([[['a', -2], 1]]))
      graph.run()

      // Third update: add back to 'a'
      input.sendData(new MultiSet([[['a', 7], 1]]))
      graph.run()

      const data = messages.map((m) => m.getInner())

      expect(data).toEqual([
        // First update: a=2, b=10
        [
          [['a', 2], 1],
          [['b', 10], 1],
        ],
        // Second update: a becomes 0 (filtered out), only removal
        [
          [['a', 2], -1], // Remove old sum for a
        ],
        // Third update: a=7 (0+7)
        [
          [['a', 7], 1], // Add new sum for a
        ],
      ])
    })
  })
})
