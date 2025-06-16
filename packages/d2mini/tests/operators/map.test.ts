import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { Message, MessageType } from '../../src/types.js'
import { map, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Map operation', () => {
    test('basic map operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: Message<number>[] = []

      input.pipe(
        map((x) => x + 5),
        output((message) => {
          messages.push(message)
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

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: {
            version: v([1, 0]),
            collection: new MultiSet([
              [6, 1],
              [7, 1],
              [8, 1],
            ]),
          },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })

    test('map with multiple transformations', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: Message<number>[] = []

      input.pipe(
        map((x) => x * 2),
        map((x) => x + 1),
        output((message) => {
          messages.push(message)
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

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: {
            version: v([1, 0]),
            collection: new MultiSet([
              [3, 1],
              [5, 1],
              [7, 1],
            ]),
          },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })

    test('map with negative multiplicities', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: Message<number>[] = []

      input.pipe(
        map((x) => x + 1),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(
        v([1, 0]),
        new MultiSet([
          [1, -1],
          [2, -2],
          [3, 1],
        ]),
      )
      input.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: {
            version: v([1, 0]),
            collection: new MultiSet([
              [2, -1],
              [3, -2],
              [4, 1],
            ]),
          },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })
  })
})
