import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { count } from '../../src/operators-sqlite.js'
import { output } from '../../src/operators/index.js'
import Database from 'better-sqlite3'
import fs from 'fs'
import path from 'path'

const DB_FILENAME = 'test-count.db'

describe('SQLite Operators', () => {
  describe('Count operation', () => {
    let db: Database.Database

    beforeEach(() => {
      db = new Database(':memory:')
    })

    afterEach(() => {
      db.close()
    })

    test('basic count operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[number, string]>()
      const messages: DataMessage<[number, number]>[] = []

      input.pipe(
        count(db),
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

    test('count with multiple versions', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, string]>()
      const messages: DataMessage<[string, number]>[] = []

      input.pipe(
        count(db),
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
          [['one', 2], -1],
          [['two', 1], 1],
        ],
      ])
    })
  })

  describe('Count operation with persistence', () => {
    const dbPath = path.join(import.meta.dirname, DB_FILENAME)
    let db: Database.Database

    beforeEach(() => {
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath)
      }
      db = new Database(dbPath)
    })

    afterEach(() => {
      db.close()
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath)
      }
    })

    test('persists and recovers state', () => {
      // First graph instance - initial processing
      let messages: DataMessage<[string, number]>[] = []
      let graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, string]>()

      input.pipe(
        count(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send initial data
      input.sendData(
        v([1, 0]),
        new MultiSet([
          [['key1', 'a'], 1],
          [['key1', 'b'], 1],
          [['key2', 'x'], 2],
        ]),
      )
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.run()

      // Verify initial results
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [['key1', 2], 1],
          [['key2', 2], 1],
        ],
      ])

      // Close first graph instance and database
      db.close()

      // Create new graph instance with same database
      messages = []
      db = new Database(dbPath)
      graph = new D2({ initialFrontier: v([1, 0]) })
      const newInput = graph.newInput<[string, string]>()

      newInput.pipe(
        count(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send new data
      newInput.sendData(
        v([2, 0]),
        new MultiSet([
          [['key1', 'c'], 1],
          [['key3', 'y'], 1],
        ]),
      )
      newInput.sendFrontier(new Antichain([v([3, 0])]))

      graph.run()

      // Verify that new results work with persisted state
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [['key1', 3], 1],
          [['key1', 2], -1],
          [['key3', 1], 1],
        ],
      ])

      // Query the database directly to verify persistence
      const tables = db
        .prepare(
          `
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name LIKE 'reduce_%'
      `,
        )
        .all()

      expect(tables.length).toBeGreaterThan(0)
    })
  })
})
