import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { Message, MessageType } from '../../src/types'

describe('Operators', () => {
  describe('Map operation', () => {
    test('basic map operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: Message<number>[] = []

      // Create a simple pipeline that adds 5 and filters even numbers
      const output = input
        .map((x) => x + 5)
        .output((message) => {
          messages.push(message)
        })

      const graph = graphBuilder.finalize()

      // Send initial data
      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
          [3, 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: Message<number>[] = []

      const output = input
        .map((x) => x * 2)
        .map((x) => x + 1)
        .output((message) => {
          messages.push(message)
        })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
          [3, 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: Message<number>[] = []

      const output = input
        .map((x) => x + 1)
        .output((message) => {
          messages.push(message)
        })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [1, -1],
          [2, -2],
          [3, 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
