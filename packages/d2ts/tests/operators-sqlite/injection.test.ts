import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType, KeyValue } from '../../src/types.js'
import { output, map } from '../../src/operators/index.js'
import {
  reduce,
  distinct,
  consolidate,
  buffer,
  filterBy,
  withSQLite,
} from '../../src/sqlite/operators/index.js'
import Database from 'better-sqlite3'
import { BetterSQLite3Wrapper } from '../../src/sqlite/database.js'

describe('SQLite Dependency Injection - withSQLite', () => {
  let db: BetterSQLite3Wrapper

  beforeEach(() => {
    const sqlite = new Database(':memory:')
    db = new BetterSQLite3Wrapper(sqlite)
  })

  afterEach(() => {
    db.close()
  })

  test('with reduce operator', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<KeyValue<string, number>>()
    const messages: DataMessage<KeyValue<string, number>>[] = []

    input.pipe(
      withSQLite(db)(
        reduce<string, number, number, KeyValue<string, number>>((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        consolidate<KeyValue<string, number>>(),
      ),
      output<KeyValue<string, number>>((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', 1], 2],
        [['a', 2], 1],
        [['a', 3], 1],
        [['b', 4], 1],
      ]),
    )
    input.sendData(v([1, 0]), new MultiSet([[['b', 5], 1]]))
    input.sendFrontier(new Antichain([v([2, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [['a', 7], 1],
        [['b', 9], 1],
      ],
    ])
  })

  test('with multiple operators', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<KeyValue<string, number>>()
    const messages: DataMessage<KeyValue<string, number>>[] = []

    input.pipe(
      withSQLite(db)(
        map<KeyValue<string, number>, KeyValue<string, number>>(
          ([key, val]) => [key, val * 2] as [string, number],
        ),
        reduce<string, number, number, KeyValue<string, number>>((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }),
        distinct<string, number, KeyValue<string, number>>(),
      ),
      output<KeyValue<string, number>>((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', 1], 1],
        [['a', 2], 1],
        [['b', 3], 1],
      ]),
    )
    input.sendFrontier(new Antichain([v([2, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    // Map doubles the values, then reduce sums them:
    // a: (1*2) + (2*2) = 6
    // b: (3*2) = 6
    // Then distinct leaves a single value for each key
    expect(data).toEqual([
      [
        [['a', 6], 1],
        [['b', 6], 1],
      ],
    ])
  })

  test('mixing explicit and injected db parameters', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<KeyValue<string, number>>()
    const messages: DataMessage<KeyValue<string, number>>[] = []

    // Use explicit db parameter for reduce
    const reducedStream = input.pipe(
      reduce<string, number, number, KeyValue<string, number>>((vals) => {
        let sum = 0
        for (const [val, diff] of vals) {
          sum += val * diff
        }
        return [[sum, 1]]
      }, db),
    )

    // Use injection for distinct
    reducedStream.pipe(
      withSQLite(db)(distinct<string, number, KeyValue<string, number>>()),
      output<KeyValue<string, number>>((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', 1], 2],
        [['a', 2], 1],
        [['b', 3], 1],
      ]),
    )
    input.sendFrontier(new Antichain([v([2, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [['a', 4], 1],
        [['b', 3], 1],
      ],
    ])
  })

  test('with buffer operator', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<number>()
    const messages: DataMessage<number>[] = []

    input.pipe(
      withSQLite(db)(buffer<number>()),
      output<number>((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet([
        [1, 1],
        [2, 1],
      ]),
    )
    input.sendData(v([2, 0]), new MultiSet([[3, 1]]))
    input.sendFrontier(new Antichain([v([3, 0])]))

    graph.run()

    const data = messages.map((m) => m.collection.getInner())
    expect(data).toHaveLength(2)
    expect(data[0]).toEqual([
      [1, 1],
      [2, 1],
    ])
    expect(data[1]).toEqual([[3, 1]])
  })

  test('with filterBy operator', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input1 =
      graph.newInput<KeyValue<string, { id: string; name: string }>>()
    const input2 = graph.newInput<KeyValue<string, { id: string }>>()
    const messages: DataMessage<
      KeyValue<string, { id: string; name: string }>
    >[] = []

    // First pipe just adds keys to filter by
    input2.pipe(map(([key, value]) => [key, {}] as KeyValue<string, {}>))

    // Second pipe filters values that match keys from input2
    input1.pipe(
      withSQLite(db)(
        filterBy<
          string,
          { id: string; name: string },
          KeyValue<string, { id: string; name: string }>
        >(input2),
      ),
      output<KeyValue<string, { id: string; name: string }>>((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      }),
    )

    graph.finalize()

    // Add some initial items
    input1.sendData(
      v([1, 0]),
      new MultiSet([
        [['key1', { id: '1', name: 'Item 1' }], 1],
        [['key2', { id: '2', name: 'Item 2' }], 1],
        [['key3', { id: '3', name: 'Item 3' }], 1],
      ]),
    )

    // Only filter by key1 and key3
    input2.sendData(
      v([1, 0]),
      new MultiSet([
        [['key1', { id: '1' }], 1],
        [['key3', { id: '3' }], 1],
      ]),
    )

    input1.sendFrontier(new Antichain([v([2, 0])]))
    input2.sendFrontier(new Antichain([v([2, 0])]))

    graph.run()

    // From the debug logs, we can see the data structure is:
    // [[[["key1",{"id":"1","name":"Item 1"}],1],[["key3",{"id":"3","name":"Item 3"}],1]]]
    // so we need to navigate the nested arrays

    // Get the result data
    const data = messages.map((m) => m.collection.getInner())[0]
    console.log('FilterBy Test Data:', JSON.stringify(data))

    // Check that we got data
    expect(data).toBeDefined()

    // Extract the actual keys
    const filteredKeys = new Set<string>()

    // Handle the deeply nested structure from the debug output
    for (const item of data) {
      // Each item is [["key", {...}], diff]
      if (Array.isArray(item) && item.length >= 1) {
        const keyValuePair = item[0]
        if (Array.isArray(keyValuePair) && keyValuePair.length >= 1) {
          filteredKeys.add(String(keyValuePair[0]))
        }
      }
    }

    // Verify the keys we received
    expect(filteredKeys.has('key1')).toBe(true)
    expect(filteredKeys.has('key3')).toBe(true)
    expect(filteredKeys.has('key2')).toBe(false)
    expect(filteredKeys.size).toBe(2)
  })
})
