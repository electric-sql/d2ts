import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'

describe('Operators', () => {
  describe('Consolidate operation', () => {
    test('basic consolidate operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = input.consolidate().output((message) => {
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
      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [3, 1],
          [4, 1],
        ]),
      )
      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [3, 2],
          [2, -1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 1])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, 1],
          [3, 3],
          [4, 1],
        ],
      ])
    })

    test('consolidate with all removed', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = input.consolidate().output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 2],
        ]),
      )
      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [1, -1],
          [2, -2],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([])
    })

    test('consolidate with multiple versions', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      let messages: DataMessage<number>[] = []

      const output = input.consolidate().output((message) => {
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
      writer.sendData(
        v([2, 0]),
        new MultiSet([
          [2, 1],
          [3, 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([3, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, 1],
          [2, 1],
        ],
        [
          [2, 1],
          [3, 1],
        ],
      ])
    })
  })
})
