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
  })
})
