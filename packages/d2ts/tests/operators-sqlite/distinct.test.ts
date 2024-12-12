import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/pipe'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'
import { distinct } from '../../src/operators-sqlite'
import { output } from '../../src/operators'
import Database from 'better-sqlite3'

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
      let messages: DataMessage<[number, string]>[] = []

      input.pipe(
        distinct(db),
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
          [[1, 'a'], 2],
          [[2, 'b'], 1],
          [[2, 'c'], 2],
        ])
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
      let messages: DataMessage<[number, string]>[] = []

      input.pipe(
        distinct(db),
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
          [[1, 'a'], 1],
          [[1, 'b'], 1],
        ])
      )
      input.sendData(
        v([2, 0]),
        new MultiSet([
          [[1, 'b'], -1],
          [[1, 'c'], 1],
        ])
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
}) 