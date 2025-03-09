import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { MessageType } from '../../src/types.js'
import {
  orderBy as inMemoryOrderBy,
  output,
} from '../../src/operators/index.js'
import { orderBy as sqliteOrderBy } from '../../src/sqlite/operators/orderBy.js'
import { KeyValue } from '../../src/types.js'
import { BetterSQLite3Wrapper } from '../../src/sqlite/database.js'
import Database from 'better-sqlite3'

describe('Operators', () => {
  describe('OrderBy operation', () => {
    testOrderBy(inMemoryOrderBy)
  })
})

describe('SQLite Operators', () => {
  describe('OrderBy operation', () => {
    let db: BetterSQLite3Wrapper

    beforeEach(() => {
      const sqlite = new Database(':memory:')
      db = new BetterSQLite3Wrapper(sqlite)
    })

    afterEach(() => {
      db.close()
    })

    const wrappedOrderBy = ((stream, options) => {
      // @ts-ignore
      return sqliteOrderBy(stream, {
        ...options,
        db: db,
      })
    }) as typeof inMemoryOrderBy

    testOrderBy(wrappedOrderBy)
  })
})

function testOrderBy(orderBy: typeof inMemoryOrderBy) {
  test('initial results with default comparator', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key2', { id: 2, value: 'z' }], 1],
        [['key3', { id: 3, value: 'b' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key5', { id: 5, value: 'c' }], 1],
      ]),
    )
    input.sendFrontier(1)

    graph.run()

    expect(latestMessage).not.toBeNull()

    const result = latestMessage.collection.getInner()

    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'a' }], 1],
      [['key2', { id: 2, value: 'z' }], 1],
      [['key3', { id: 3, value: 'b' }], 1],
      [['key4', { id: 4, value: 'y' }], 1],
      [['key5', { id: 5, value: 'c' }], 1],
    ])
  })

  test('initial results with custom comparator', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value, {
        comparator: (a, b) => b.localeCompare(a), // reverse order
      }),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key2', { id: 2, value: 'z' }], 1],
        [['key3', { id: 3, value: 'b' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key5', { id: 5, value: 'c' }], 1],
      ]),
    )
    input.sendFrontier(1)

    graph.run()

    expect(latestMessage).not.toBeNull()

    const result = latestMessage.collection.getInner()

    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'a' }], 1],
      [['key2', { id: 2, value: 'z' }], 1],
      [['key3', { id: 3, value: 'b' }], 1],
      [['key4', { id: 4, value: 'y' }], 1],
      [['key5', { id: 5, value: 'c' }], 1],
    ])
  })

  test('initial results with limit', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value, { limit: 3 }),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key2', { id: 2, value: 'z' }], 1],
        [['key3', { id: 3, value: 'b' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key5', { id: 5, value: 'c' }], 1],
      ]),
    )
    input.sendFrontier(1)

    graph.run()

    expect(latestMessage).not.toBeNull()

    const result = latestMessage.collection.getInner()

    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'a' }], 1],
      [['key3', { id: 3, value: 'b' }], 1],
      [['key5', { id: 5, value: 'c' }], 1],
    ])
  })

  test('initial results with limit and offset', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value, { limit: 2, offset: 2 }),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key2', { id: 2, value: 'z' }], 1],
        [['key3', { id: 3, value: 'b' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key5', { id: 5, value: 'c' }], 1],
      ]),
    )
    input.sendFrontier(1)

    graph.run()

    expect(latestMessage).not.toBeNull()

    const result = latestMessage.collection.getInner()

    expect(sortResults(result)).toEqual([
      [['key4', { id: 4, value: 'y' }], 1],
      [['key5', { id: 5, value: 'c' }], 1],
    ])
  })

  test('ordering by numeric property', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.id),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 5, value: 'a' }], 1],
        [['key2', { id: 3, value: 'z' }], 1],
        [['key3', { id: 1, value: 'b' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key5', { id: 2, value: 'c' }], 1],
      ]),
    )
    input.sendFrontier(1)

    graph.run()

    expect(latestMessage).not.toBeNull()

    const result = latestMessage.collection.getInner()

    expect(sortResults(result)).toEqual([
      [['key1', { id: 5, value: 'a' }], 1],
      [['key2', { id: 3, value: 'z' }], 1],
      [['key3', { id: 1, value: 'b' }], 1],
      [['key4', { id: 4, value: 'y' }], 1],
      [['key5', { id: 2, value: 'c' }], 1],
    ])
  })

  test('incremental update - adding new rows', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    // Initial data
    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'c' }], 1],
        [['key2', { id: 2, value: 'd' }], 1],
        [['key3', { id: 3, value: 'e' }], 1],
      ]),
    )
    input.sendFrontier(1)
    graph.run()

    // Initial result should be all three items in alphabetical order
    let result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'c' }], 1],
      [['key2', { id: 2, value: 'd' }], 1],
      [['key3', { id: 3, value: 'e' }], 1],
    ])

    // Add new rows that should appear in the result
    input.sendData(
      1,
      new MultiSet([
        [['key4', { id: 4, value: 'a' }], 1],
        [['key5', { id: 5, value: 'b' }], 1],
      ]),
    )
    input.sendFrontier(2)
    graph.run()

    // Result should now include the new rows in the correct order
    result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key4', { id: 4, value: 'a' }], 1], // New row
      [['key5', { id: 5, value: 'b' }], 1], // New row
    ])
  })

  test('incremental update - removing rows', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    // Initial data
    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key2', { id: 2, value: 'b' }], 1],
        [['key3', { id: 3, value: 'c' }], 1],
        [['key4', { id: 4, value: 'd' }], 1],
      ]),
    )
    input.sendFrontier(1)
    graph.run()

    // Initial result should be all four items
    let result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'a' }], 1],
      [['key2', { id: 2, value: 'b' }], 1],
      [['key3', { id: 3, value: 'c' }], 1],
      [['key4', { id: 4, value: 'd' }], 1],
    ])

    // Remove 'b' from the result set
    input.sendData(1, new MultiSet([[['key2', { id: 2, value: 'b' }], -1]]))
    input.sendFrontier(2)
    graph.run()

    // Result should show 'b' being removed
    result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key2', { id: 2, value: 'b' }], -1], // Removed row
    ])
  })

  test('incremental update - with limit', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value, { limit: 3 }),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    // Initial data
    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'c' }], 1],
        [['key2', { id: 2, value: 'd' }], 1],
        [['key3', { id: 3, value: 'e' }], 1],
      ]),
    )
    input.sendFrontier(1)
    graph.run()

    // Initial result should be all three items
    let result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'c' }], 1],
      [['key2', { id: 2, value: 'd' }], 1],
      [['key3', { id: 3, value: 'e' }], 1],
    ])

    // Add a new row that should appear in the result (before 'c')
    input.sendData(1, new MultiSet([[['key4', { id: 4, value: 'a' }], 1]]))
    input.sendFrontier(2)
    graph.run()

    // Result should now include 'a' and drop 'e' due to limit
    result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key3', { id: 3, value: 'e' }], -1], // Moved out of the limit so it's removed
      [['key4', { id: 4, value: 'a' }], 1], // New row at the beginning
    ])
  })

  test('incremental update - with limit and offset', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value, { limit: 2, offset: 1 }),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    // Initial data
    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key2', { id: 2, value: 'b' }], 1],
        [['key3', { id: 3, value: 'c' }], 1],
        [['key4', { id: 4, value: 'd' }], 1],
      ]),
    )
    input.sendFrontier(1)
    graph.run()

    // Initial result should be items at positions 1 and 2 (b and c)
    let result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key2', { id: 2, value: 'b' }], 1],
      [['key3', { id: 3, value: 'c' }], 1],
    ])

    // Add a new row that should appear at the beginning
    input.sendData(1, new MultiSet([[['key5', { id: 5, value: '_' }], 1]]))
    input.sendFrontier(2)
    graph.run()

    // Result should now shift: a is out, _ is in at offset 1, b is still in, c is out
    result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'a' }], 1], // Now in the window due to offset
      [['key3', { id: 3, value: 'c' }], -1], // Moved out due to window shift
    ])
  })

  test('incremental update - modifying existing rows', () => {
    const graph = new D2({ initialFrontier: 0 })
    const input = graph.newInput<
      KeyValue<
        string,
        {
          id: number
          value: string
        }
      >
    >()
    let latestMessage: any = null

    input.pipe(
      orderBy((item) => item.value),
      output((message) => {
        if (message.type === MessageType.DATA) {
          latestMessage = message.data
        }
      }),
    )

    graph.finalize()

    // Initial data
    input.sendData(
      0,
      new MultiSet([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key2', { id: 2, value: 'c' }], 1],
        [['key3', { id: 3, value: 'e' }], 1],
      ]),
    )
    input.sendFrontier(1)
    graph.run()

    // Initial result
    let result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key1', { id: 1, value: 'a' }], 1],
      [['key2', { id: 2, value: 'c' }], 1],
      [['key3', { id: 3, value: 'e' }], 1],
    ])

    // Modify an existing row by removing it and adding a new version
    input.sendData(
      1,
      new MultiSet([
        [['key2', { id: 2, value: 'c' }], -1], // Remove old version
        [['key2', { id: 2, value: 'z' }], 1], // Add new version with different value
      ]),
    )
    input.sendFrontier(2)
    graph.run()

    // Result should show the modification
    result = latestMessage.collection.getInner()
    expect(sortResults(result)).toEqual([
      [['key2', { id: 2, value: 'c' }], -1], // Old version removed
      [['key2', { id: 2, value: 'z' }], 1], // New version added
    ])
  })
}

/**
 * Sort results by multiplicity and then key
 */
function sortResults(results: any[]) {
  return [...results]
    .sort(
      ([[_aKey, _aValue], aMultiplicity], [[_bKey, _bValue], bMultiplicity]) =>
        aMultiplicity - bMultiplicity,
    )
    .sort(
      ([[aKey, _aValue], _aMultiplicity], [[bKey, _bValue], _bMultiplicity]) =>
        aKey.localeCompare(bKey),
    )
}
