import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { output } from '../../src/operators/index.js'
import { joinAll } from '../../src/operators/join-all.js'

// Tell TypeScript to ignore type errors in this file since we're testing runtime behavior
describe('Operators', () => {
  describe('JoinAll operation', () => {
    testJoinAll()
  })
})

function testJoinAll() {
  test('should join multiple streams with inner join', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[string, number]>()
    const inputB = graph.newInput<[string, string]>()
    const inputC = graph.newInput<[string, boolean]>()
    const messages: DataMessage<any>[] = []

    inputA.pipe(
      joinAll([inputB, inputC]),
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
        [['a', 10], 1],
        [['b', 20], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))

    inputB.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', 'hello'], 1],
        [['c', 'world'], 1],
      ]),
    )
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    inputC.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', true], 1],
        [['b', false], 1],
      ]),
    )
    inputC.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    // Inner join should only include records that match on all streams
    expect(data).toEqual([[[['a', [10, 'hello', true]], 1]]])
  })

  test('should join multiple streams with left join', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[string, number]>()
    const inputB = graph.newInput<[string, string]>()
    const inputC = graph.newInput<[string, boolean]>()
    const messages: DataMessage<any>[] = []

    inputA.pipe(
      joinAll([inputB, inputC], 'left'),
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
        [['a', 10], 1],
        [['b', 20], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))

    inputB.sendData(v([1, 0]), new MultiSet([[['a', 'hello'], 1]]))
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    inputC.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', true], 1],
        [['b', false], 1],
      ]),
    )
    inputC.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    // Left join should include all keys from first stream
    const expected = [
      [
        [['a', [10, 'hello', true]], 1],
        [['b', [20, null, false]], 1],
      ],
    ]

    // Sort the arrays for consistent comparison
    const sortedData = data.map((arr) =>
      [...arr].sort((a, b) => a[0][0].localeCompare(b[0][0])),
    )
    const sortedExpected = expected.map((arr) =>
      [...arr].sort((a, b) => a[0][0].localeCompare(b[0][0])),
    )

    expect(sortedData).toEqual(sortedExpected)
  })

  test('should handle empty array of other streams', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[string, number]>()
    const messages: DataMessage<any>[] = []

    inputA.pipe(
      joinAll([]),
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
        [['a', 10], 1],
        [['b', 20], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    // Should return the original stream values wrapped in arrays
    expect(data).toEqual([
      [
        [['a', [10]], 1],
        [['b', [20]], 1],
      ],
    ])
  })

  test('should join streams across multiple versions', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[string, number]>()
    const inputB = graph.newInput<[string, string]>()
    const inputC = graph.newInput<[string, boolean]>()
    const messages: DataMessage<any>[] = []

    inputA.pipe(
      joinAll([inputB, inputC]),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    // Version 1
    inputA.sendData(v([1, 0]), new MultiSet([[['a', 10], 1]]))
    inputA.sendFrontier(new Antichain([v([1, 0])]))

    inputB.sendData(v([1, 0]), new MultiSet([[['a', 'hello'], 1]]))
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    inputC.sendData(v([1, 0]), new MultiSet([[['a', true], 1]]))
    inputC.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    // Version 2
    inputA.sendData(v([2, 0]), new MultiSet([[['b', 20], 1]]))
    inputA.sendFrontier(new Antichain([v([2, 0])]))

    inputB.sendData(v([2, 0]), new MultiSet([[['b', 'world'], 1]]))
    inputB.sendFrontier(new Antichain([v([2, 0])]))

    inputC.sendData(v([2, 0]), new MultiSet([[['b', false], 1]]))
    inputC.sendFrontier(new Antichain([v([2, 0])]))

    graph.run()

    const data = messages.map((m) => {
      return {
        version: m.version.toString(),
        collection: m.collection.getInner(),
      }
    })

    // Sort the arrays for consistent comparison
    const sortedData = data.map((item) => ({
      version: item.version,
      collection: [...item.collection].sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      ),
    }))

    // Should have 2 versions with correct joined results
    expect(sortedData.length).toBe(2)
    // We can't directly compare the version string, so check the data matches expected format
    expect(sortedData[0].version).toContain('[1,0]')
    expect(sortedData[0].collection).toEqual([[['a', [10, 'hello', true]], 1]])
    expect(sortedData[1].version).toContain('[2,0]')
    expect(sortedData[1].collection).toEqual([[['b', [20, 'world', false]], 1]])
  })
}
