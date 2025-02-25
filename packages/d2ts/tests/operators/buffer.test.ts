import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, MessageType } from '../../src/types.js'
import { buffer, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Buffer operation', () => {
    test('basic buffer operation', () => {
      const graph = new D2({ initialFrontier: v([0]) })
      const input = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      input.pipe(
        buffer(),
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
})
