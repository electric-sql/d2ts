import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { reduce, output } from '../../src/operators.js'

describe('Operators', () => {
  describe('Reduce operation', () => {
    test('basic reduce operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, number]>()
      const messages: DataMessage<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      input.sendData(
        v([1, 0]),
        new MultiSet([
          [['a', 1], 2],
          [['a', 2], 1],
          [['a', 3], 1],
          [['b', 4], 1],
        ]),
      )
      input.sendData(v([1, 0]), new MultiSet([[['b', 5], 1]]))
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [['a', 7], 1],
          [['b', 9], 1],
        ],
      ])
    })

    test('reduce with negative multiplicities', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, number]>()
      const messages: DataMessage<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      input.sendData(
        v([1, 0]),
        new MultiSet([
          [['a', 1], -1],
          [['a', 2], 2],
          [['b', 3], -2],
        ]),
      )
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [['a', 3], 1],
          [['b', -6], 1],
        ],
      ])
    })
  })
})
