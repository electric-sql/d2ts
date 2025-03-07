import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { MessageType } from '../../src/types.js'
import { orderBy, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('OrderBy operation', () => {
    test('initial order with limit - no key', () => {
      const graph = new D2({ initialFrontier: 0})
      const input = graph.newInput<[null, {
        id: number // 1, 2, 3, 4, 5
        value: string // a, z, b, y, c
      }]>()
      let latestMessage: any = null

      input.pipe(
        orderBy((a, b) => a.value.localeCompare(b.value), { limit: 3 }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'z' }], 1],
          [[null, { id: 3, value: 'b' }], 1],
          [[null, { id: 4, value: 'y' }], 1],
          [[null, { id: 5, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(sortResults(result)).toEqual([
        [[null, { id: 1, value: 'a' }], 1],
        [[null, { id: 3, value: 'b' }], 1],
        [[null, { id: 5, value: 'c' }], 1],
      ])
    })

    test('initial order with limit and offset - no key', () => {
      const graph = new D2({ initialFrontier: 0})
      const input = graph.newInput<[null, {
        id: number // 1, 2, 3, 4, 5
        value: string // a, z, b, y, c
      }]>()
      let latestMessage: any = null
      input.pipe(
        orderBy((a, b) => a.value.localeCompare(b.value), { limit: 3, offset: 2 }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'z' }], 1],
          [[null, { id: 3, value: 'b' }], 1],
          [[null, { id: 4, value: 'y' }], 1],
          [[null, { id: 5, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(sortResults(result)).toEqual([
        [[null, { id: 2, value: 'z' }], 1],
        [[null, { id: 3, value: 'b' }], 1],
        [[null, { id: 4, value: 'y' }], 1],
      ])
    })

    test('initial order with limit - with key', () => {
      const graph = new D2({ initialFrontier: 0})
      const input = graph.newInput<[string, {
        id: number // 1, 2, 3, 4, 5
        value: string // a, z, b, y, c
      }]>()
      let latestMessage: any = null

      input.pipe(
        orderBy((a, b) => a.value.localeCompare(b.value), { limit: 3 }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [['one', { id: 1, value: '9' }], 1],
          [['one', { id: 2, value: '8' }], 1],
          [['one', { id: 3, value: '7' }], 1],
          [['one', { id: 4, value: '6' }], 1],
          [['one', { id: 5, value: '5' }], 1],
          [['two', { id: 6, value: '4' }], 1],
          [['two', { id: 7, value: '3' }], 1],
          [['two', { id: 8, value: '2' }], 1],
          [['two', { id: 9, value: '1' }], 1],
          [['two', { id: 10, value: '0' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(sortResults(result)).toEqual([
        [['one', { id: 3, value: '7' }], 1],
        [['one', { id: 4, value: '6' }], 1],
        [['one', { id: 5, value: '5' }], 1],
        [['two', { id: 8, value: '2' }], 1],
        [['two', { id: 9, value: '1' }], 1],
        [['two', { id: 10, value: '0' }], 1],
      ])
    })

    test('initial order with limit and offset - with key', () => {
      const graph = new D2({ initialFrontier: 0})
      const input = graph.newInput<[string, {
        id: number // 1, 2, 3, 4, 5
        value: string // a, z, b, y, c
      }]>()
      let latestMessage: any = null

      input.pipe(
        orderBy((a, b) => a.value.localeCompare(b.value), { limit: 3, offset: 2 }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [['one', { id: 1, value: '9' }], 1],
          [['one', { id: 2, value: '8' }], 1],
          [['one', { id: 3, value: '7' }], 1],
          [['one', { id: 4, value: '6' }], 1],
          [['one', { id: 5, value: '5' }], 1],
          [['two', { id: 6, value: '4' }], 1],
          [['two', { id: 7, value: '3' }], 1],
          [['two', { id: 8, value: '2' }], 1],
          [['two', { id: 9, value: '1' }], 1],
          [['two', { id: 10, value: '0' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(sortResults(result)).toEqual([
        [['one', { id: 1, value: '9' }], 1],
        [['one', { id: 2, value: '8' }], 1],
        [['one', { id: 3, value: '7' }], 1],
        [['two', { id: 6, value: '4' }], 1],
        [['two', { id: 7, value: '3' }], 1],
        [['two', { id: 8, value: '2' }], 1],
      ])
    })
  })
})

/**
 * Helper function to sort results by multiplicity and then id
 * This is necessary as the implementation does not guarantee order of the messages
 * only that the materialization is correct
 */
function sortResults(results: any[]) {
  return results
    .sort(
      ([_a, aMultiplicity], [_b, bMultiplicity]) =>
        aMultiplicity - bMultiplicity,
    )
    .sort(
      ([_a, { id: aId }], [_b, { id: bId }]) =>
        aId - bId,
    )
}