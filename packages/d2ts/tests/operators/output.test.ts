import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { Message, MessageType } from '../../src/types.js'
import { output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Output operation', () => {
    test('basic output operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: Message<number>[] = []

      input.pipe(
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      input.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: { version: v([1, 0]), collection: new MultiSet([[1, 1]]) },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })

    test('output with multiple versions', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: Message<number>[] = []

      input.pipe(
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      input.sendData(v([2, 0]), new MultiSet([[2, 1]]))
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.run()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: { version: v([1, 0]), collection: new MultiSet([[1, 1]]) },
        },
        {
          type: MessageType.DATA,
          data: { version: v([2, 0]), collection: new MultiSet([[2, 1]]) },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([2, 0])]) },
      ])
    })

    test('output with empty collection', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: Message<number>[] = []

      input.pipe(
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      input.sendData(v([1, 0]), new MultiSet([]))
      input.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: { version: v([1, 0]), collection: new MultiSet([]) },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })
  })
})
