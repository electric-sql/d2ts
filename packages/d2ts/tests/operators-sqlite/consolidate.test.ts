import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/pipe'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'
import { consolidate } from '../../src/operators-sqlite'
import { output } from '../../src/operators'
import Database from 'better-sqlite3'
import fs from 'fs'
import path from 'path'

describe('SQLite Operators', () => {
  describe('Consolidate operation', () => {
    let db: Database.Database

    beforeEach(() => {
      db = new Database(':memory:')
    })

    afterEach(() => {
      db.close()
    })

    test('basic consolidate operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      let messages: DataMessage<number>[] = []

      input.pipe(
        consolidate(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        })
      )

      graph.finalize()

      input.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
        ])
      )
      input.sendData(
        v([1, 0]),
        new MultiSet([
          [3, 1],
          [4, 1],
        ])
      )
      input.sendData(
        v([1, 0]),
        new MultiSet([
          [3, 2],
          [2, -1],
        ])
      )
      input.sendFrontier(new Antichain([v([1, 1])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, 1],
          [3, 3],
          [4, 1],
        ],
      ])
    })

    test('consolidate with multiple versions', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      let messages: DataMessage<number>[] = []

      input.pipe(
        consolidate(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        })
      )

      graph.finalize()

      input.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 1],
        ])
      )
      input.sendData(
        v([2, 0]),
        new MultiSet([
          [2, 1],
          [3, 1],
        ])
      )
      input.sendFrontier(new Antichain([v([3, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [1, 1],
          [2, 1],
        ],
        [
          [2, 1],
          [3, 1],
        ],
      ])
    })
  })

  describe('Consolidate operation with persistence', () => {
    const dbPath = path.join(__dirname, 'test.db')
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
      let messages: DataMessage<number>[] = []
      let graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()

      input.pipe(
        consolidate(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        })
      )

      graph.finalize()

      // Send initial data
      input.sendData(
        v([1, 0]),
        new MultiSet([
          [1, 1],
          [2, 2],
        ])
      )

      graph.step()

      // Close first graph instance and database
      db.close()

      // Create new graph instance with same database
      messages = []
      db = new Database(dbPath)
      graph = new D2({ initialFrontier: v([1, 0]) })
      const newInput = graph.newInput<number>()

      newInput.pipe(
        consolidate(db),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        })
      )

      graph.finalize()

      // Send new data
      newInput.sendData(
        v([1, 0]),
        new MultiSet([
          [2, 1],
          [3, 3],
        ])
      )
      newInput.sendFrontier(new Antichain([v([2, 0])]))

      graph.step()

      // Verify that new results work with persisted state
      expect(messages.map((m) => m.collection.getInner())).toEqual([
        [
          [1, 1],
          [2, 3],
          [3, 3],
        ],
      ])
    })
  })
})
