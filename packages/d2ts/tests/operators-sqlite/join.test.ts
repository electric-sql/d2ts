import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { join } from '../../src/sqlite/operators/join.js'
import { output } from '../../src/operators/index.js'
import Database from 'better-sqlite3'
import { BetterSQLite3Wrapper } from '../../src/sqlite/database.js'
import fs from 'fs'
import path from 'path'

const DB_FILENAME = 'test-join.db'

describe('SQLite Operators', () => {
  describe('Join operation', () => {
    let db: BetterSQLite3Wrapper

    beforeEach(() => {
      const sqlite = new Database(':memory:')
      db = new BetterSQLite3Wrapper(sqlite)
    })

    afterEach(() => {
      db.close()
    })

    test('basic join operation (inner join)', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const inputA = graph.newInput<[number, string]>()
      const inputB = graph.newInput<[number, string]>()
      const messages: DataMessage<[number, [string | null, string | null]]>[] =
        []

      inputA.pipe(
        join(inputB, db),
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
      const messages: DataMessage<[number, [string | null, string | null]]>[] =
        []

      inputA.pipe(
        join(inputB, db, 'left'),
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
      const messages: DataMessage<[number, [string | null, string | null]]>[] =
        []

      inputA.pipe(
        join(inputB, db, 'right'),
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
      const messages: DataMessage<[number, [string | null, string | null]]>[] =
        []

      inputA.pipe(
        join(inputB, db, 'full'),
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
      const messages: DataMessage<[number, [string | null, string | null]]>[] =
        []

      inputA.pipe(
        join(inputB, db),
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
  })

  describe('Join operation with persistence', () => {
    const dbPath = path.join(import.meta.dirname, DB_FILENAME)
    let db: BetterSQLite3Wrapper

    beforeEach(() => {
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath)
      }
      const sqlite = new Database(dbPath)
      db = new BetterSQLite3Wrapper(sqlite)
    })

    afterEach(() => {
      db.close()
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath)
      }
    })

    test('persists and recovers state', () => {
      // First graph instance - initial processing
      let messages: DataMessage<[number, [string | null, string | null]]>[] = []
      let graph = new D2({ initialFrontier: v([0, 0]) })
      const inputA = graph.newInput<[number, string]>()
      const inputB = graph.newInput<[number, string]>()

      inputA.pipe(
        join(inputB, db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send initial data
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
        ]),
      )
      inputB.sendFrontier(new Antichain([v([1, 0])]))

      graph.run()

      // Verify initial results
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [[1, ['a', 'x']], 1],
          [[2, ['b', 'y']], 1],
        ],
      ])

      // Close first graph instance and database
      db.close()

      // Create new graph instance with same database
      messages = []
      db = new BetterSQLite3Wrapper(new Database(dbPath))
      graph = new D2({ initialFrontier: v([1, 0]) })
      const newInputA = graph.newInput<[number, string]>()
      const newInputB = graph.newInput<[number, string]>()

      newInputA.pipe(
        join(newInputB, db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send new data
      newInputA.sendData(v([2, 0]), new MultiSet([[[2, 'c'], 1]]))
      newInputA.sendFrontier(new Antichain([v([2, 0])]))
      newInputB.sendFrontier(new Antichain([v([2, 0])]))

      graph.run()

      // Verify that new results include joins with previously persisted data
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [[2, ['c', 'y']], 1], // y is from the previous data
        ],
      ])
    })
  })
})
