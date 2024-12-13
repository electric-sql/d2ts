import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { reduce } from '../../src/sqlite/operators/reduce.js'
import { output } from '../../src/operators/index.js'
import Database from 'better-sqlite3'
import fs from 'fs'
import path from 'path'

describe('SQLite Operators', () => {
  describe('Reduce operation', () => {
    let db: Database.Database

    beforeEach(() => {
      db = new Database(':memory:')
    })

    afterEach(() => {
      db.close()
    })

    test('basic reduce operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, number]>()
      const messages: DataMessage<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }, db),
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

    test('reduce with negative multiplicities', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, number]>()
      const messages: DataMessage<[string, number]>[] = []

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }, db),
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
          [['a', 1], -1],
          [['a', 2], 2],
          [['b', 3], -2],
        ]),
      )
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.run()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [['a', 3], 1],
          [['b', -6], 1],
        ],
      ])
    })
  })

  describe('Reduce operation with persistence', () => {
    const dbPath = path.join(import.meta.dirname, 'test.db')
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
      const input = graph.newInput<[string, number]>()

      input.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }, db),
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
          [['a', 1], 2],
          [['a', 2], 1],
          [['b', 3], 1],
        ]),
      )
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.run()

      // Verify initial results
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [['a', 4], 1],
          [['b', 3], 1],
        ],
      ])

      // Close first graph instance and database
      db.close()

      // Create new graph instance with same database
      messages = []
      db = new Database(dbPath)
      graph = new D2({ initialFrontier: v([1, 0]) })
      const newInput = graph.newInput<[string, number]>()

      newInput.pipe(
        reduce((vals) => {
          let sum = 0
          for (const [val, diff] of vals) {
            sum += val * diff
          }
          return [[sum, 1]]
        }, db),
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
          [['a', 3], 1],
          [['c', 5], 1],
        ]),
      )
      newInput.sendFrontier(new Antichain([v([3, 0])]))

      graph.run()

      // Verify that new results work with persisted state
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [['a', 7], 1],
          [['a', 4], -1],
          [['c', 5], 1],
        ],
      ])
    })
  })
})
