import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { Message, MessageType } from '../../src/types'

describe('Operators', () => {
  describe('Output operation', () => {
    test('basic output operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: Message<number>[] = []

      const output = input.output((message) => {
        messages.push(message)
      })

      const graph = graphBuilder.finalize()

      writer.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

      expect(messages).toEqual([
        {
          type: MessageType.DATA,
          data: { version: v([1, 0]), collection: new MultiSet([[1, 1]]) },
        },
        { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
      ])
    })

    test('output with multiple versions', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: Message<number>[] = []

      const output = input.output((message) => {
        messages.push(message)
      })

      const graph = graphBuilder.finalize()

      writer.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      writer.sendData(v([2, 0]), new MultiSet([[2, 1]]))
      writer.sendFrontier(new Antichain([v([2, 0])]))

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

    test('output with empty collection', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: Message<number>[] = []

      const output = input.output((message) => {
        messages.push(message)
      })

      const graph = graphBuilder.finalize()

      writer.sendData(v([1, 0]), new MultiSet([]))
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
