import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/pipe'
import { MultiSet } from '../../src/multiset'
import { filter, map, output } from '../../src/operators'
import { Message, MessageType } from '../../src/types'
import { v, Antichain } from '../../src/order'

describe('Operators - in-memory', () => {
  describe('Filter operation', () => {
    test('basic filter operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      let messages: Message<number>[] = []

      input.pipe(
        filter((x) => x % 2 === 0),
        output((message) => {
          messages.push(message)
        })
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

      graph.step()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: { version: v([1, 0]), collection: new MultiSet([[2, 1]]) },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })

    test('filter with complex predicate', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      let messages: Message<number>[] = []

      input.pipe(
        filter((x) => x > 2 && x < 5),
        output((message) => {
          messages.push(message)
        })
      )

      graph.finalize()

      input.sendData([1, 0], new MultiSet([
        [1, 1],
        [2, 1],
        [3, 1],
        [4, 1],
        [5, 1],
      ]))
      input.sendFrontier(v([1, 0]))

      graph.step()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: {
            version: v([1, 0]),
            collection: new MultiSet([
              [3, 1],
              [4, 1],
            ]),
          },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })

    test('filter with chained operations', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      let messages: Message<number>[] = []

      input.pipe(
        map((x) => x * 2),
        filter((x) => x % 4 === 0),
        output((message) => {
          messages.push(message)
        })
      )

      graph.finalize()

      input.sendData([1, 0], new MultiSet([
        [1, 1],
        [2, 1],
        [3, 1],
        [4, 1],
      ]))
      input.sendFrontier(v([1, 0]))

      graph.step()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: {
            version: v([1, 0]),
            collection: new MultiSet([
              [4, 1],
              [8, 1],
            ]),
          },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })
  })
})
