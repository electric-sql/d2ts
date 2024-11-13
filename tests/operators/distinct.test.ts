import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'

describe('Operators', () => {
  describe('Distinct operation', () => {
    test('basic distinct operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<[number, string]>()

      let messages: DataMessage<[number, string]>[] = []

      const output = input.distinct().output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'a'], 2],
          [[2, 'b'], 1],
          [[2, 'c'], 2],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 1])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [[1, 'a'], 1],
          [[2, 'b'], 1],
          [[2, 'c'], 1],
        ],
      ])
    })

    test('distinct with updates', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<[number, string]>()

      let messages: DataMessage<[number, string]>[] = []

      const output = input.distinct().output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'a'], 1],
          [[1, 'b'], 1],
        ]),
      )
      writer.sendData(
        v([2, 0]),
        new MultiSet([
          [[1, 'b'], -1],
          [[1, 'c'], 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([3, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [[1, 'a'], 1],
          [[1, 'b'], 1],
        ],
        [
          [[1, 'c'], 1],
          [[1, 'b'], -1],
        ],
      ])
    })

    test('distinct with multiple versions of same key', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<[string, number]>()

      let messages: DataMessage<[string, number]>[] = []

      const output = input.distinct().output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [['key1', 1], 2],
          [['key1', 2], 3],
          [['key2', 1], 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([2, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [['key1', 1], 1],
          [['key1', 2], 1],
          [['key2', 1], 1],
        ],
      ])
    })
  })
})
