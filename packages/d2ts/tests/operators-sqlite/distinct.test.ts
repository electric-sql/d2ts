import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { distinct } from '../../src/operators-sqlite.js'
import { output } from '../../src/operators.js'
import Database from 'better-sqlite3'
import fs from 'fs'
import path from 'path'

const DB_FILENAME = 'test-distinct.db'

describe('SQLite Operators', () => {
  describe('Distinct operation', () => {
    let db: Database.Database

    beforeEach(() => {
      db = new Database(':memory:')
    })

    afterEach(() => {
      db.close()
    })

    test('basic distinct operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[number, string]>()
      const messages: DataMessage<[number, string]>[] = []

      input.pipe(
        distinct(db),
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

      graph.step()

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
        distinct(db),
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

      graph.step()

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
  })

  describe('Distinct operation with persistence', () => {
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
      let messages: DataMessage<[string, string]>[] = []
      let graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, string]>()

      input.pipe(
        distinct(db),
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
          [['key1', 'a'], 2],
          [['key1', 'b'], 1],
          [['key2', 'x'], 3],
        ]),
      )
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.step()

      // Verify initial results
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [['key1', 'a'], 1],
          [['key1', 'b'], 1],
          [['key2', 'x'], 1],
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
        distinct(db),
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
          [['key1', 'b'], -1],
          [['key1', 'c'], 2],
          [['key3', 'y'], 1],
        ]),
      )
      newInput.sendFrontier(new Antichain([v([3, 0])]))

      graph.step()

      // Verify that new results work with persisted state
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [['key1', 'c'], 1],
          [['key1', 'b'], -1],
          [['key3', 'y'], 1],
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
