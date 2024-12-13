import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { join, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Join operation', () => {
    testJoin()
  })
})

function testJoin() {
  test('basic join operation', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    inputA.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))

    inputB.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
        [[3, 'z'], 1],
      ]),
    )
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])
  })

  test('join with late arriving data', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    inputA.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    inputB.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
      ]),
    )
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])
  })

  test('join with negative multiplicities', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    inputA.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], -1],
      ]),
    )
    inputB.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], -1],
      ],
    ])
  })
}
