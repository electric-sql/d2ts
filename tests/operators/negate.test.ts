import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'

describe('Operators', () => {
  describe('Negate operation', () => {
    test('basic negate operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = input.negate().output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = input.negate().output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [1, -2],
          [2, 1],
          [3, -3],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = input
        .map((x) => x * 2)
        .negate()
        .map((x) => x + 1)
        .output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

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
