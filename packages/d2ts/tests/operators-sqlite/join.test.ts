import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { join } from '../../src/sqlite/operators.js'
import { output } from '../../src/operators/index.js'
import Database from 'better-sqlite3'
import fs from 'fs'
import path from 'path'

const DB_FILENAME = 'test-join.db'

describe('SQLite Operators', () => {
  describe('Join operation', () => {
    let db: Database.Database

    beforeEach(() => {
      db = new Database(':memory:')
    })

    afterEach(() => {
      db.close()
    })

    test('basic join operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const inputA = graph.newInput<[number, string]>()
      const inputB = graph.newInput<[number, string]>()
      const messages: DataMessage<[number, [string, string]]>[] = []

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
    let db: Database.Database

    beforeEach(() => {
      // Clean up any existing test database
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
      let messages: DataMessage<[number, [string, string]]>[] = []
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
      db = new Database(dbPath)
      graph = new D2({ initialFrontier: v([1, 0]) }) // Start from last frontier
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
