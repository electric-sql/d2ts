import { describe, test, expect, beforeAll } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import {
  orderByWithFractionalIndex,
  output,
} from '../../src/operators/index.js'
import { orderByWithFractionalIndexBTree } from '../../src/operators/orderByBTree.js'
import { KeyValue } from '../../src/types.js'
import { loadBTree } from '../../src/operators/topKWithFractionalIndexBTree.js'
import { MessageTracker } from '../test-utils.js'

const stripFractionalIndex = ([[key, [value, _index]], multiplicity]) => [
  key,
  value,
  multiplicity,
]

beforeAll(async () => {
  await loadBTree()
})

describe('Operators', () => {
  describe.each([
    ['with array', { orderBy: orderByWithFractionalIndex }],
    ['with B+ tree', { orderBy: orderByWithFractionalIndexBTree }],
  ])('OrderByWithFractionalIndex operator %s', (_, { orderBy }) => {
    test('initial results with default comparator', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy((item) => item.value),
        output((message) => {
          latestMessage = message
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.getInner()
      const sortedResult = sortByKeyAndIndex(result).map(stripFractionalIndex)

      expect(sortedResult).toEqual([
        ['key1', { id: 1, value: 'a' }, 1],
        ['key3', { id: 3, value: 'b' }, 1],
        ['key5', { id: 5, value: 'c' }, 1],
        ['key4', { id: 4, value: 'y' }, 1],
        ['key2', { id: 2, value: 'z' }, 1],
      ])
    })

    test('initial results with custom comparator', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy((item) => item.value, {
          comparator: (a, b) => b.localeCompare(a), // reverse order
        }),
        output((message) => {
          latestMessage = message
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.getInner()
      const sortedResult = sortByKeyAndIndex(result).map(stripFractionalIndex)

      expect(sortedResult).toEqual([
        ['key2', { id: 2, value: 'z' }, 1],
        ['key4', { id: 4, value: 'y' }, 1],
        ['key5', { id: 5, value: 'c' }, 1],
        ['key3', { id: 3, value: 'b' }, 1],
        ['key1', { id: 1, value: 'a' }, 1],
      ])
    })

    test('initial results with limit', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy((item) => item.value, { limit: 3 }),
        output((message) => {
          latestMessage = message
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.getInner()
      const sortedResult = sortByKeyAndIndex(result).map(stripFractionalIndex)

      expect(sortedResult).toEqual([
        ['key1', { id: 1, value: 'a' }, 1],
        ['key3', { id: 3, value: 'b' }, 1],
        ['key5', { id: 5, value: 'c' }, 1],
      ])
    })

    test('initial results with limit and offset', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy((item) => item.value, {
          limit: 2,
          offset: 2,
        }),
        output((message) => {
          latestMessage = message
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.getInner()
      const sortedResult = sortByKeyAndIndex(result).map(stripFractionalIndex)

      expect(sortedResult).toEqual([
        ['key5', { id: 5, value: 'c' }, 1],
        ['key4', { id: 4, value: 'y' }, 1],
      ])
    })

    test('ordering by numeric property', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy((item) => item.id),
        output((message) => {
          latestMessage = message
        }),
      )

      graph.finalize()

      input.sendData(
        new MultiSet([
          [['key5', { id: 5, value: 'e' }], 1],
          [['key3', { id: 3, value: 'c' }], 1],
          [['key1', { id: 1, value: 'a' }], 1],
          [['key4', { id: 4, value: 'd' }], 1],
          [['key2', { id: 2, value: 'b' }], 1],
        ]),
      )

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.getInner()
      const sortedResult = sortByKeyAndIndex(result).map(stripFractionalIndex)

      expect(sortedResult).toEqual([
        ['key1', { id: 1, value: 'a' }, 1],
        ['key2', { id: 2, value: 'b' }, 1],
        ['key3', { id: 3, value: 'c' }, 1],
        ['key4', { id: 4, value: 'd' }, 1],
        ['key5', { id: 5, value: 'e' }, 1],
      ])
    })

    test('incremental update - adding a new row', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy((item) => item.value, { limit: 3 }),
        output((message) => {
          latestMessage = message
        }),
      )

      graph.finalize()

      // Initial data
      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key3', { id: 3, value: 'c' }], 1],
          [['key2', { id: 2, value: 'b' }], 1],
        ]),
      )
      graph.run()

      expect(latestMessage).not.toBeNull()

      const initialResult = latestMessage.getInner()
      const sortedInitialResult =
        sortByKeyAndIndex(initialResult).map(stripFractionalIndex)

      expect(sortedInitialResult).toEqual([
        ['key1', { id: 1, value: 'a' }, 1],
        ['key2', { id: 2, value: 'b' }, 1],
        ['key3', { id: 3, value: 'c' }, 1],
      ])

      // Add a new row that should be included in the top 3
      input.sendData(
        new MultiSet([
          [['key4', { id: 4, value: 'aa' }], 1], // Should be second in order
        ]),
      )
      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.getInner()
      const sortedResult = sortByKeyAndIndex(result).map(stripFractionalIndex)

      expect(sortedResult).toEqual([
        // We dont get key1 as its not changed or moved
        ['key4', { id: 4, value: 'aa' }, 1], // New row
        ['key3', { id: 3, value: 'c' }, -1], // key3 is removed as its moved out of top 3
      ])
    })

    test('incremental update - removing a row', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      const tracker = new MessageTracker<[string, [{ id: number; value: string }, string]]>()

      input.pipe(
        orderBy((item) => item.value, { limit: 3 }),
        output((message) => {
          tracker.addMessage(message)
        }),
      )

      graph.finalize()

      // Initial data
      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key3', { id: 3, value: 'c' }], 1],
          [['key2', { id: 2, value: 'b' }], 1],
          [['key4', { id: 4, value: 'd' }], 1],
        ]),
      )
      graph.run()

      const initialResult = tracker.getResult()
      console.log(`orderBy initial: ${initialResult.messageCount} messages, ${initialResult.sortedResults.length} final results`)

      // Should have the top 3 items by value
      expect(initialResult.sortedResults.length).toBe(3)
      expect(initialResult.messageCount).toBeLessThanOrEqual(4) // Should be efficient

      tracker.reset()

      // Remove a row that was in the top 3
      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], -1], // Remove the first item
        ]),
      )
      graph.run()

      const updateResult = tracker.getResult()
      console.log(`orderBy remove: ${updateResult.messageCount} messages, ${updateResult.sortedResults.length} final results`)

      // Should have efficient incremental update
      expect(updateResult.messageCount).toBeLessThanOrEqual(4) // Should be incremental
      expect(updateResult.messageCount).toBeGreaterThan(0) // Should have changes

      // Check that only affected keys produce messages - should be key1 (removed) and key4 (added to top 3)
      const affectedKeys = new Set(updateResult.messages.map(([[key, _value], _mult]) => key))
      expect(affectedKeys.size).toBeLessThanOrEqual(2) // Should only affect key1 and key4
      
      // Verify specific keys are affected
      for (const key of affectedKeys) {
        expect(['key1', 'key4'].includes(key)).toBe(true)
      }
    })

    test('incremental update - modifying a row', () => {
      const graph = new D2()
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      const tracker = new MessageTracker<[string, [{ id: number; value: string }, string]]>()

      input.pipe(
        orderBy((item) => item.value, { limit: 3 }),
        output((message) => {
          tracker.addMessage(message)
        }),
      )

      graph.finalize()

      // Initial data
      input.sendData(
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'c' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'd' }], 1],
        ]),
      )
      graph.run()

      const initialResult = tracker.getResult()
      console.log(`orderBy modify initial: ${initialResult.messageCount} messages, ${initialResult.sortedResults.length} final results`)

      // Should have the top 3 items by value
      expect(initialResult.sortedResults.length).toBe(3)
      expect(initialResult.messageCount).toBeLessThanOrEqual(4) // Should be efficient

      tracker.reset()

      // Modify an existing row by removing it and adding a new version
      input.sendData(
        new MultiSet([
          [['key2', { id: 2, value: 'c' }], -1], // Remove old version
          [['key2', { id: 2, value: 'z' }], 1], // Add new version with different value
        ]),
      )
      graph.run()

      const updateResult = tracker.getResult()
      console.log(`orderBy modify update: ${updateResult.messageCount} messages, ${updateResult.sortedResults.length} final results`)

      // Should have efficient incremental update
      expect(updateResult.messageCount).toBeLessThanOrEqual(6) // Should be incremental (modify operation)
      expect(updateResult.messageCount).toBeGreaterThan(0) // Should have changes

      // Check that only affected keys produce messages - should be key2 (modified) and key4 (added to top 3)  
      const affectedKeys = new Set(updateResult.messages.map(([[key, _value], _mult]) => key))
      expect(affectedKeys.size).toBeLessThanOrEqual(2) // Should only affect key2 and key4
      
      // Verify specific keys are affected
      for (const key of affectedKeys) {
        expect(['key2', 'key4'].includes(key)).toBe(true)
      }
    })
  })
})

/**
 * Helper function to sort results by key and then index
 */
function sortByKeyAndIndex(results: any[]) {
  return [...results]
    .sort(
      (
        [[_aKey, [_aValue, _aIndex]], aMultiplicity],
        [[_bKey, [_bValue, _bIndex]], bMultiplicity],
      ) => aMultiplicity - bMultiplicity,
    )
    .sort(
      (
        [[aKey, [_aValue, _aIndex]], _aMultiplicity],
        [[bKey, [_bValue, _bIndex]], _bMultiplicity],
      ) => aKey - bKey,
    )
    .sort(
      (
        [[_aKey, [_aValue, aIndex]], _aMultiplicity],
        [[_bKey, [_bValue, bIndex]], _bMultiplicity],
      ) => {
        // lexically compare the index
        //return aIndex.localeCompare(bIndex)
        return aIndex < bIndex ? -1 : aIndex > bIndex ? 1 : 0
      },
    )
}
