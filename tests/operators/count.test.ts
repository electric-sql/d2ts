import { describe, test, expect } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'
import Database from 'better-sqlite3'

describe('Operators - in-memory', () => {
  describe('Count operation', () => {
    testCount()
  })
})

describe('Operators - sqlite', () => {
  const newDb = () => new Database(':memory:')
  describe('Count operation', () => {
    testCount(newDb)
  })
})

function testCount(newDb?: () => InstanceType<typeof Database>) {
  test('basic count operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]), newDb?.())
    const [input, writer] = graphBuilder.newInput<[number, string]>()

    let messages: DataMessage<[number, number]>[] = []

    const output = input.count().output((message) => {
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
        [[2, 'c'], 1],
        [[2, 'd'], 1],
        [[3, 'x'], 1],
        [[3, 'y'], -1],
      ]),
    )
    writer.sendData(v([1, 0]), new MultiSet([[[3, 'z'], 1]]))
    writer.sendFrontier(new Antichain([v([2, 1])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, 2], 1],
        [[2, 3], 1],
        [[3, 1], 1],
      ],
    ])
  })

  test('count with all negative multiplicities', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]), newDb?.())
    const [input, writer] = graphBuilder.newInput<[number, string]>()

    let messages: DataMessage<[number, number]>[] = []

    const output = input.count().output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], -1],
        [[1, 'b'], -2],
      ]),
    )
    writer.sendFrontier(new Antichain([v([2, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([[[[1, -3], 1]]])
  })

  test('count with multiple versions', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]), newDb?.())
    const [input, writer] = graphBuilder.newInput<[string, string]>()

    let messages: DataMessage<[string, number]>[] = []

    const output = input.count().output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [['one', 'a'], 1],
        [['one', 'b'], 1],
      ]),
    )
    writer.sendData(
      v([2, 0]),
      new MultiSet([
        [['one', 'c'], 1],
        [['two', 'a'], 1],
      ]),
    )
    writer.sendFrontier(new Antichain([v([3, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [[['one', 2], 1]],
      [
        [['one', 3], 1],
        [['one', 2], -1], // <-- old count of 'one' removed
        [['two', 1], 1],
      ],
    ])
  })
}