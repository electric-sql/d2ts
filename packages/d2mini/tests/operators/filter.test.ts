import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet, IMultiSet } from '../../src/multiset.js'
import { filter, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Filter operation', () => {
    test('basic filter operation', () => {
      const graph = new D2()
      const input = graph.newInput<number>()
      const messages: IMultiSet<number>[] = []

      input.pipe(
        filter((x) => x % 2 === 0),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [1, 1],
          [2, 1],
          [3, 1],
        ]),
      )

      graph.run()

      expect(messages.map(m => m.getInner())).toEqual([[[2, 1]]])
    })

    test('filter with complex predicate', () => {
      const graph = new D2()
      const input = graph.newInput<number>()
      const messages: IMultiSet<number>[] = []

      input.pipe(
        filter((x) => x > 2 && x < 5),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [1, 1],
          [2, 1],
          [3, 1],
          [4, 1],
          [5, 1],
        ]),
      )

      graph.run()

      expect(messages.map(m => m.getInner())).toEqual([
        [
          [3, 1],
          [4, 1],
        ],
      ])
    })

    test('filter with chained operations', () => {
      const graph = new D2()
      const input = graph.newInput<number>()
      const messages: IMultiSet<number>[] = []

      input.pipe(
        filter((x) => x % 2 === 0),
        filter((x) => x > 2),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [1, 1],
          [2, 1],
          [3, 1],
          [4, 1],
          [5, 1],
          [6, 1],
        ]),
      )

      graph.run()

      expect(messages.map(m => m.getInner())).toEqual([
        [
          [4, 1],
          [6, 1],
        ],
      ])
    })
  })
})
