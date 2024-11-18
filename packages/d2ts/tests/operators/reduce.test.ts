import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'
import Database from 'better-sqlite3'

describe('Operators - in-memory', () => {
  describe('Reduce operation', () => {
    testReduce()
  })
})

describe('Operators - sqlite', () => {
  const newDb = () => new Database(':memory:')
  describe('Reduce operation', () => {
    testReduce(newDb)
  })
})

function testReduce(newDb?: () => InstanceType<typeof Database>) {
  test('basic reduce operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]), newDb?.())
    const [input, writer] = graphBuilder.newInput<[string, number]>()

    let messages: DataMessage<[string, number]>[] = []

    const output = input
      .reduce((vals) => {
        let sum = 0
        for (const [val, diff] of vals) {
          sum += val * diff
        }
        return [[sum, 1]]
      })
      .output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', 1], 2],
        [['a', 2], 1],
        [['a', 3], 1],
        [['b', 4], 1],
      ]),
    )
    writer.sendData(v([1, 0]), new MultiSet([[['b', 5], 1]]))
    writer.sendFrontier(new Antichain([v([2, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [['a', 7], 1],
        [['b', 9], 1],
      ],
    ])
  })

  test('reduce with negative multiplicities', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]), newDb?.())
    const [input, writer] = graphBuilder.newInput<[string, number]>()

    let messages: DataMessage<[string, number]>[] = []

    const output = input
      .reduce((vals) => {
        let sum = 0
        for (const [val, diff] of vals) {
          sum += val * diff
        }
        return [[sum, 1]]
      })
      .output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', 1], -1],
        [['a', 2], 2],
        [['b', 3], -2],
      ]),
    )
    writer.sendFrontier(new Antichain([v([2, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [['a', 3], 1],
        [['b', -6], 1],
      ],
    ])
  })
}
