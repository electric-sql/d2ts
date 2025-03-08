import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { MessageType } from '../../src/types.js'
import { indexedTopK, output } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('IndexedTopK operation', () => {
    test('initial results with limit - no key', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        [
          null,
          {
            id: number
            value: string
          },
        ]
      >()
      let latestMessage: any = null

      input.pipe(
        indexedTopK((a, b) => a.value.localeCompare(b.value), { limit: 3 }),
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
      const sortedResult = sortByIndexAndId(result)

      expect(sortedResult).toEqual([
        [[null, [{ id: 1, value: 'a' }, 0]], 1],
        [[null, [{ id: 3, value: 'b' }, 1]], 1],
        [[null, [{ id: 5, value: 'c' }, 2]], 1],
      ])
    })

    test('initial results with limit and offset - no key', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        [
          null,
          {
            id: number
            value: string
          },
        ]
      >()
      let latestMessage: any = null
      input.pipe(
        indexedTopK((a, b) => a.value.localeCompare(b.value), {
          limit: 3,
          offset: 2,
        }),
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
      const sortedResult = sortByIndexAndId(result)

      expect(sortedResult).toEqual([
        [[null, [{ id: 5, value: 'c' }, 2]], 1],
        [[null, [{ id: 4, value: 'y' }, 3]], 1],
        [[null, [{ id: 2, value: 'z' }, 4]], 1],
      ])
    })

    test('initial results with limit - with key', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        [
          string,
          {
            id: number
            value: string
          },
        ]
      >()
      let latestMessage: any = null

      input.pipe(
        indexedTopK((a, b) => a.value.localeCompare(b.value), { limit: 3 }),
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
      const sortedResult = sortByKeyIndexAndId(result)

      expect(sortedResult).toEqual([
        [['one', [{ id: 5, value: '5' }, 0]], 1],
        [['one', [{ id: 4, value: '6' }, 1]], 1],
        [['one', [{ id: 3, value: '7' }, 2]], 1],
        [['two', [{ id: 10, value: '0' }, 0]], 1],
        [['two', [{ id: 9, value: '1' }, 1]], 1],
        [['two', [{ id: 8, value: '2' }, 2]], 1],
      ])
    })

    test('incremental update - removing a row', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        [
          null,
          {
            id: number
            value: string
          },
        ]
      >()
      let latestMessage: any = null

      input.pipe(
        indexedTopK((a, b) => a.value.localeCompare(b.value), { limit: 3 }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      // Initial data
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result should be first three items with indices
      let result = latestMessage.collection.getInner()
      let sortedResult = sortByIndexAndId(result)
      expect(sortedResult).toEqual([
        [[null, [{ id: 1, value: 'a' }, 0]], 1],
        [[null, [{ id: 2, value: 'b' }, 1]], 1],
        [[null, [{ id: 3, value: 'c' }, 2]], 1],
      ])

      // Remove 'b' from the result set
      input.sendData(1, new MultiSet([[[null, { id: 2, value: 'b' }], -1]]))
      input.sendFrontier(2)
      graph.run()

      // Result should show 'b' being removed with its old index,
      // 'c' moving from index 2 to 1, and 'd' being added at index 2
      result = latestMessage.collection.getInner()
      sortedResult = sortByMultiplicityIndexAndId(result)

      expect(sortedResult).toEqual([
        [[null, [{ id: 2, value: 'b' }, 1]], -1], // Removed row with its old index
        [[null, [{ id: 3, value: 'c' }, 2]], -1], // 'c' removed from old index 2
        [[null, [{ id: 3, value: 'c' }, 1]], 1], // 'c' moved from index 2 to 1
        [[null, [{ id: 4, value: 'd' }, 2]], 1], // New row added at index 2
      ])
    })

    test('incremental update - adding rows that push existing rows out of limit window', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        [
          null,
          {
            id: number
            value: string
          },
        ]
      >()
      let latestMessage: any = null

      input.pipe(
        indexedTopK((a, b) => a.value.localeCompare(b.value), { limit: 3 }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      // Initial data
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'c' }], 1],
          [[null, { id: 2, value: 'd' }], 1],
          [[null, { id: 3, value: 'e' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result should be all three items with indices
      let result = latestMessage.collection.getInner()
      let sortedResult = sortByIndexAndId(result)
      expect(sortedResult).toEqual([
        [[null, [{ id: 1, value: 'c' }, 0]], 1],
        [[null, [{ id: 2, value: 'd' }, 1]], 1],
        [[null, [{ id: 3, value: 'e' }, 2]], 1],
      ])

      // Add two new rows that should appear before existing rows
      input.sendData(
        1,
        new MultiSet([
          [[null, { id: 4, value: 'a' }], 1],
          [[null, { id: 5, value: 'b' }], 1],
        ]),
      )
      input.sendFrontier(2)
      graph.run()

      // Result should show:
      // - 'a' and 'b' being added at indices 0 and 1
      // - 'c' moving from index 0 to 2
      // - 'd' and 'e' being removed as they're pushed out of the limit window
      result = latestMessage.collection.getInner()
      sortedResult = sortByMultiplicityIndexAndId(result)

      expect(sortedResult).toEqual([
        [[null, [{ id: 1, value: 'c' }, 0]], -1], // 'c' removed from old index 0
        [[null, [{ id: 2, value: 'd' }, 1]], -1], // 'd' removed from index 1
        [[null, [{ id: 3, value: 'e' }, 2]], -1], // 'e' removed from index 2
        [[null, [{ id: 4, value: 'a' }, 0]], 1], // New row at index 0
        [[null, [{ id: 5, value: 'b' }, 1]], 1], // New row at index 1
        [[null, [{ id: 1, value: 'c' }, 2]], 1], // 'c' added at new index 2
      ])
    })

    test('incremental update - changing a value that affects ordering', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        [
          null,
          {
            id: number
            value: string
          },
        ]
      >()
      let latestMessage: any = null

      input.pipe(
        indexedTopK((a, b) => a.value.localeCompare(b.value), { limit: 3 }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      // Initial data
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result should be all three items with indices
      let result = latestMessage.collection.getInner()
      let sortedResult = sortByIndexAndId(result)
      expect(sortedResult).toEqual([
        [[null, [{ id: 1, value: 'a' }, 0]], 1],
        [[null, [{ id: 2, value: 'b' }, 1]], 1],
        [[null, [{ id: 3, value: 'c' }, 2]], 1],
      ])

      // Change 'a' to 'z' which should move it to the end, outside the limit
      input.sendData(
        1,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], -1],
          [[null, { id: 1, value: 'z' }], 1],
        ]),
      )
      input.sendFrontier(2)
      graph.run()

      // Result should show:
      // - 'a' being removed from index 0
      // - 'b' moving from index 1 to 0
      // - 'c' moving from index 2 to 1
      // - 'z' being added at index 2
      result = latestMessage.collection.getInner()
      sortedResult = sortByMultiplicityIndexAndId(result)

      expect(sortedResult).toEqual([
        [[null, [{ id: 1, value: 'a' }, 0]], -1], // 'a' removed from index 0
        [[null, [{ id: 2, value: 'b' }, 1]], -1], // 'b' removed from old index 1
        [[null, [{ id: 3, value: 'c' }, 2]], -1], // 'c' removed from old index 2
        [[null, [{ id: 2, value: 'b' }, 0]], 1], // 'b' added at new index 0
        [[null, [{ id: 3, value: 'c' }, 1]], 1], // 'c' added at new index 1
        [[null, [{ id: 1, value: 'z' }, 2]], 1], // 'z' added at index 2
      ])
    })

    test('incremental update with offset - items moving in and out of window', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        [
          null,
          {
            id: number
            value: string
          },
        ]
      >()
      let latestMessage: any = null

      input.pipe(
        indexedTopK((a, b) => a.value.localeCompare(b.value), {
          limit: 2,
          offset: 1,
        }),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result should be b, c (offset 1, limit 2)
      let result = latestMessage.collection.getInner()
      let sortedResult = sortByIndexAndId(result)
      expect(sortedResult).toEqual([
        [[null, [{ id: 2, value: 'b' }, 1]], 1],
        [[null, [{ id: 3, value: 'c' }, 2]], 1],
      ])

      // Add a new item 'aa' that should be between 'a' and 'b'
      input.sendData(1, new MultiSet([[[null, { id: 6, value: 'aa' }], 1]]))
      input.sendFrontier(2)
      graph.run()

      // Result should show:
      // - 'aa' being added at index 1
      // - 'b' moving from index 1 to 2
      // - 'c' being removed as it's pushed out of the window
      result = latestMessage.collection.getInner()
      sortedResult = sortByMultiplicityIndexAndId(result)

      expect(sortedResult).toEqual([
        [[null, [{ id: 2, value: 'b' }, 1]], -1], // 'b' removed from old index 1
        [[null, [{ id: 3, value: 'c' }, 2]], -1], // 'c' removed from index 2
        [[null, [{ id: 6, value: 'aa' }, 1]], 1], // 'aa' added at index 1
        [[null, [{ id: 2, value: 'b' }, 2]], 1], // 'b' added at new index 2
      ])
    })
  })
})

/**
 * Helper function to sort results by index and then id
 */
function sortByIndexAndId(results: any[]) {
  return [...results].sort(
    (
      [[_aKey, [aValue, aIndex]], _aMultiplicity],
      [[_bKey, [bValue, bIndex]], _bMultiplicity],
    ) => {
      // First sort by index
      if (aIndex !== bIndex) {
        return aIndex - bIndex
      }
      // Then by id if indices are the same
      return aValue.id - bValue.id
    },
  )
}

/**
 * Helper function to sort results by key, then index, then id
 */
function sortByKeyIndexAndId(results: any[]) {
  return [...results].sort(
    (
      [[aKey, [aValue, aIndex]], _aMultiplicity],
      [[bKey, [bValue, bIndex]], _bMultiplicity],
    ) => {
      // First sort by key
      if (aKey !== bKey) {
        return aKey < bKey ? -1 : 1
      }
      // Then by index
      if (aIndex !== bIndex) {
        return aIndex - bIndex
      }
      // Then by id if indices are the same
      return aValue.id - bValue.id
    },
  )
}

/**
 * Helper function to sort results by multiplicity, then index, then id
 */
function sortByMultiplicityIndexAndId(results: any[]) {
  return [...results].sort(
    (
      [[_aKey, [aValue, aIndex]], aMultiplicity],
      [[_bKey, [bValue, bIndex]], bMultiplicity],
    ) => {
      // First sort by multiplicity
      if (aMultiplicity !== bMultiplicity) {
        return aMultiplicity - bMultiplicity
      }
      // Then by index
      if (aIndex !== bIndex) {
        return aIndex - bIndex
      }
      // Then by id if indices are the same
      return aValue.id - bValue.id
    },
  )
}
