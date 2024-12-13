import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { distinct, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Distinct operation', () => {
    testDistinct()
  })
})

function testDistinct() {
  test('basic distinct operation', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, string]>[] = []

    input.pipe(
      distinct(),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 2],
        [[2, 'b'], 1],
        [[2, 'c'], 2],
      ]),
    )
    input.sendFrontier(new Antichain([v([1, 1])]))

    graph.run()

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
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, string]>[] = []

    input.pipe(
      distinct(),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[1, 'b'], 1],
      ]),
    )
    input.sendData(
      v([2, 0]),
      new MultiSet([
        [[1, 'b'], -1],
        [[1, 'c'], 1],
      ]),
    )
    input.sendFrontier(new Antichain([v([3, 0])]))

    graph.run()

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
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<[string, number]>()
    const messages: DataMessage<[string, number]>[] = []

    input.pipe(
      distinct(),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet([
        [['key1', 1], 2],
        [['key1', 2], 3],
        [['key2', 1], 1],
      ]),
    )
    input.sendFrontier(new Antichain([v([2, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [['key1', 1], 1],
        [['key1', 2], 1],
        [['key2', 1], 1],
      ],
    ])
  })
}
