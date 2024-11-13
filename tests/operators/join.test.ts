import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'

describe('Operators', () => {
  describe('Join operation', () => {
    test('basic join operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [inputA, writerA] = graphBuilder.newInput<[number, string]>()
      const [inputB, writerB] = graphBuilder.newInput<[number, string]>()

      let messages: DataMessage<[number, [string, string]]>[] = []

      const output = inputA.join(inputB).output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writerA.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'a'], 1],
          [[2, 'b'], 1],
        ]),
      )
      writerA.sendFrontier(new Antichain([v([1, 0])]))
      writerB.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'x'], 1],
          [[2, 'y'], 1],
          [[3, 'z'], 1],
        ]),
      )
      writerB.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [[1, ['a', 'x']], 1],
          [[2, ['b', 'y']], 1],
        ],
      ])
    })

    test('join with late arriving data', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [inputA, writerA] = graphBuilder.newInput<[number, string]>()
      const [inputB, writerB] = graphBuilder.newInput<[number, string]>()

      let messages: DataMessage<[number, [string, string]]>[] = []

      const output = inputA.join(inputB).output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writerA.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'a'], 1],
          [[2, 'b'], 1],
        ]),
      )
      writerA.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

      writerB.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'x'], 1],
          [[2, 'y'], 1],
        ]),
      )
      writerB.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [[1, ['a', 'x']], 1],
          [[2, ['b', 'y']], 1],
        ],
      ])
    })

    test('join with negative multiplicities', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [inputA, writerA] = graphBuilder.newInput<[number, string]>()
      const [inputB, writerB] = graphBuilder.newInput<[number, string]>()

      let messages: DataMessage<[number, [string, string]]>[] = []

      const output = inputA.join(inputB).output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

      const graph = graphBuilder.finalize()

      writerA.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'a'], 1],
          [[2, 'b'], -1],
        ]),
      )
      writerB.sendData(
        v([1, 0]),
        new MultiSet([
          [[1, 'x'], 1],
          [[2, 'y'], 1],
        ]),
      )
      writerA.sendFrontier(new Antichain([v([1, 0])]))
      writerB.sendFrontier(new Antichain([v([1, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [[1, ['a', 'x']], 1],
          [[2, ['b', 'y']], -1],
        ],
      ])
    })
  })
})
