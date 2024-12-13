import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, Message, MessageType } from '../../src/types.js'
import { concat, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Concat operation', () => {
    test('basic concat operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const inputA = graph.newInput<number>()
      const inputB = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      inputA.pipe(
        concat(inputB),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      inputA.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
        ]),
      )
      inputA.sendFrontier(new Antichain([v([1, 0])]))

      inputB.sendData(
        v([1, 0]),
        new MultiSet([
          [3, 1],
          [4, 1],
        ]),
      )
      inputB.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, 1],
          [2, 1],
        ],
        [
          [3, 1],
          [4, 1],
        ],
      ])
    })

    test('concat with overlapping data', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const inputA = graph.newInput<number>()
      const inputB = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      inputA.pipe(
        concat(inputB),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      inputA.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
          [3, 1],
        ]),
      )
      inputB.sendData(
        v([1, 0]),
        new MultiSet([
          [2, 2],
          [3, -1],
          [4, 1],
        ]),
      )
      inputA.sendFrontier(new Antichain([v([1, 0])]))
      inputB.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, 1],
          [2, 1],
          [3, 1],
        ],
        [
          [2, 2],
          [3, -1],
          [4, 1],
        ],
      ])
    })

    test('concat with different frontiers', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const inputA = graph.newInput<number>()
      const inputB = graph.newInput<number>()
      const messages: Message<number>[] = []

      inputA.pipe(
        concat(inputB),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      inputA.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      inputA.sendFrontier(new Antichain([v([1, 0])]))

      inputB.sendData(v([2, 0]), new MultiSet([[2, 1]]))
      inputB.sendFrontier(new Antichain([v([2, 0])]))

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
  })
})
