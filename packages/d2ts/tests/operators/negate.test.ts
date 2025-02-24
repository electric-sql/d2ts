import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { map, negate, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Negate operation', () => {
    test('basic negate operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      input.pipe(
        negate(),
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
          [1, 1],
          [2, 1],
          [3, 1],
        ]),
      )
      input.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, -1],
          [2, -1],
          [3, -1],
        ],
      ])
    })

    test('negate with already negative multiplicities', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      input.pipe(
        negate(),
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
          [1, -2],
          [2, 1],
          [3, -3],
        ]),
      )
      input.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, 2],
          [2, -1],
          [3, 3],
        ],
      ])
    })

    test('negate with chained operations', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      input.pipe(
        map((x) => x * 2),
        negate(),
        map((x) => x + 1),
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
          [1, 1],
          [2, 1],
        ]),
      )
      input.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [3, -1],
          [5, -1],
        ],
      ])
    })
  })
})
