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
  test('basic join operation (inner join)', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string | null, string | null]]>[] = []

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
        [[4, 'd'], 1],
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

    // Inner join should only include records that match on both sides
    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])
  })

  test('left join operation', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string | null, string | null]]>[] = []

    inputA.pipe(
      join(inputB, 'left'),
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
        [[4, 'd'], 1],
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
    // Left join should include records from the left side even if there's no match
    const expected = [
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
        [[4, ['d', null]], 1],
      ],
    ]

    // Sort the arrays for consistent comparison
    const sortedData = data.map((arr) =>
      [...arr].sort((a, b) => a[0][0] - b[0][0]),
    )
    const sortedExpected = expected.map((arr) =>
      [...arr].sort((a, b) => a[0][0] - b[0][0]),
    )

    expect(sortedData).toEqual(sortedExpected)
  })

  test('right join operation', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string | null, string | null]]>[] = []

    inputA.pipe(
      join(inputB, 'right'),
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
        [[4, 'd'], 1],
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
    // Right join should include records from the right side even if there's no match
    const expected = [
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
        [[3, [null, 'z']], 1],
      ],
    ]

    // Sort the arrays for consistent comparison
    const sortedData = data.map((arr) =>
      [...arr].sort((a, b) => a[0][0] - b[0][0]),
    )
    const sortedExpected = expected.map((arr) =>
      [...arr].sort((a, b) => a[0][0] - b[0][0]),
    )

    expect(sortedData).toEqual(sortedExpected)
  })

  test('full join operation', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string | null, string | null]]>[] = []

    inputA.pipe(
      join(inputB, 'full'),
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
        [[4, 'd'], 1],
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
    // Full join should include all records from both sides
    const expected = [
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
        [[3, [null, 'z']], 1],
        [[4, ['d', null]], 1],
      ],
    ]

    // Sort the arrays for consistent comparison
    const sortedData = data.map((arr) =>
      [...arr].sort((a, b) => a[0][0] - b[0][0]),
    )
    const sortedExpected = expected.map((arr) =>
      [...arr].sort((a, b) => a[0][0] - b[0][0]),
    )

    expect(sortedData).toEqual(sortedExpected)
  })

  test('join with late arriving data', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: DataMessage<[number, [string | null, string | null]]>[] = []

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
    const messages: DataMessage<[number, [string | null, string | null]]>[] = []

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
