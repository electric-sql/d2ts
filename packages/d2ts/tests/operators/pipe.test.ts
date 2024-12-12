import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { DataMessage, Message, MessageType } from '../../src/types.js'
import { map, output, pipe } from '../../src/operators.js'

describe('Operators', () => {
  describe('Pipe operation', () => {
    test('basic pipe operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()
      const messages: DataMessage<number>[] = []

      input.pipe(
        pipe(
          map((x) => x + 5),
          map((x) => x * 2),
        ),
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
          [1, 1],
          [2, 1],
          [3, 1],
        ]),
      )
      input.sendFrontier(new Antichain([v([2, 0])]))

      graph.step()

      const data = messages.map((m) => m.collection.getInner())

      expect(data).toEqual([
        [
          [12, 1],
          [14, 1],
          [16, 1],
        ],
      ])
    })
  })
})
