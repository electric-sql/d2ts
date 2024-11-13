import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, Message, MessageType } from '../../src/types'

describe('Operators', () => {
  describe('Concat operation', () => {
    test('basic concat operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

      const [inputA, writerA] = graphBuilder.newInput<number>()
      const [inputB, writerB] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = inputA.concat(inputB).output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writerA.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
        ]),
      )
      writerA.sendFrontier(new Antichain([v([1, 0])]))
      writerB.sendData(
        v([1, 0]),
        new MultiSet([
          [3, 1],
          [4, 1],
        ]),
      )
      writerB.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [inputA, writerA] = graphBuilder.newInput<number>()
      const [inputB, writerB] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = inputA.concat(inputB).output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writerA.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
          [3, 1],
        ]),
      )
      writerB.sendData(
        v([1, 0]),
        new MultiSet([
          [2, 2],
          [3, -1],
          [4, 1],
        ]),
      )
      writerA.sendFrontier(new Antichain([v([1, 0])]))
      writerB.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [inputA, writerA] = graphBuilder.newInput<number>()
      const [inputB, writerB] = graphBuilder.newInput<number>()

      let messages: Message<number>[] = []

      const output = inputA.concat(inputB).output((message) => {
        messages.push(message)
      })

      const graph = graphBuilder.finalize()

      writerA.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      writerA.sendFrontier(new Antichain([v([1, 0])]))

      writerB.sendData(v([2, 0]), new MultiSet([[2, 1]]))
      writerB.sendFrontier(new Antichain([v([2, 0])]))

      graph.step()

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
