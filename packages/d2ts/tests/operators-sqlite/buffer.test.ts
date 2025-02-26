import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { buffer } from '../../src/sqlite/operators/buffer.js'
import { output } from '../../src/operators/index.js'
import Database from 'better-sqlite3'
import { BetterSQLite3Wrapper } from '../../src/sqlite/database.js'
import fs from 'fs'
import path from 'path'

const DB_FILENAME = 'test-buffer.db'

describe('SQLite Operators', () => {
  describe('Buffer operation', () => {
    let db: BetterSQLite3Wrapper

    beforeEach(() => {
      const sqlite = new Database(':memory:')
      db = new BetterSQLite3Wrapper(sqlite)
    })

    afterEach(() => {
      db.close()
    })

    test('basic buffer operation', () => {
      const graph = new D2({ initialFrontier: v([0]) })
      const input = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      input.pipe(
        buffer(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      input.sendData(
        v([1]),
        new MultiSet([
          [1, 1],
          [2, 1],
        ]),
      )
      input.sendData(
        v([2]),
        new MultiSet([
          [3, 1],
          [4, 1],
        ]),
      )

      input.sendFrontier(new Antichain([v([2])]))
      graph.run()

      const data = messages.map((m) => ({
        version: m.version,
        collection: m.collection.getInner(),
      }))

      // Should output complete buffered collection for version [1]
      expect(data).toEqual([
        {
          version: v([2]),
          collection: [
            [1, 1],
            [2, 1],
          ],
        },
      ])

      messages.length = 0

      input.sendFrontier(new Antichain([v([3])]))
      graph.run()

      const data2 = messages.map((m) => ({
        version: m.version,
        collection: m.collection.getInner(),
      }))

      // Should output complete buffered collection for version [2]
      expect(data2).toEqual([
        {
          version: v([3]),
          collection: [
            [3, 1],
            [4, 1],
          ],
        },
      ])
    })
  })

  describe('Buffer operation with persistence', () => {
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
      let messages: DataMessage<number>[] = []
      let graph = new D2({ initialFrontier: v([0]) })
      const input = graph.newInput<number>()

      input.pipe(
        buffer(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send initial data
      input.sendData(
        v([1]),
        new MultiSet([
          [1, 1],
          [2, 1],
        ]),
      )

      graph.run()

      // Close first graph instance and database
      db.close()

      // Create new graph instance with same database
      messages = []
      db = new BetterSQLite3Wrapper(new Database(dbPath))
      graph = new D2({ initialFrontier: v([1]) })
      const newInput = graph.newInput<number>()

      newInput.pipe(
        buffer(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send new data
      newInput.sendData(
        v([1]),
        new MultiSet([
          [3, 1],
          [4, 1],
        ]),
      )
      newInput.sendFrontier(new Antichain([v([2])]))

      graph.run()

      // Verify that new results work with persisted state
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [1, 1],
          [2, 1],
          [3, 1],
          [4, 1],
        ],
      ])
    })
  })
})
