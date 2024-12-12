import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/pipe'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'
import { consolidate } from '../../src/operators-sqlite'
import { output } from '../../src/operators'
import Database from 'better-sqlite3'

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
}) 