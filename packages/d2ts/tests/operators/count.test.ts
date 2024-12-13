import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { count, output } from '../../src/operators.js'

describe('Operators', () => {
  describe('Count operation', () => {
    testCount()
  })
})

function testCount() {
  test('basic count operation', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, number]>[] = []

    input.pipe(
      count(),
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
        [[2, 'c'], 1],
        [[2, 'd'], 1],
        [[3, 'x'], 1],
        [[3, 'y'], -1],
      ]),
    )
    input.sendData(v([1, 0]), new MultiSet([[[3, 'z'], 1]]))
    input.sendFrontier(new Antichain([v([2, 1])]))

    graph.run()

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
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, number]>[] = []

    input.pipe(
      count(),
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
        [[1, 'a'], -1],
        [[1, 'b'], -2],
      ]),
    )
    input.sendFrontier(new Antichain([v([2, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([[[[1, -3], 1]]])
  })

  test('count with multiple versions', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<[string, string]>()
    const messages: DataMessage<[string, number]>[] = []

    input.pipe(
      count(),
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
        [['one', 'a'], 1],
        [['one', 'b'], 1],
      ]),
    )
    input.sendData(
      v([2, 0]),
      new MultiSet([
        [['one', 'c'], 1],
        [['two', 'a'], 1],
      ]),
    )
    input.sendFrontier(new Antichain([v([3, 0])]))

    graph.run()

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
