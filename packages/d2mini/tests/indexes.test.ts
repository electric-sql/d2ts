import { describe, test, expect, beforeEach } from 'vitest'
import { Index } from '../src/indexes.js'

describe('Index', () => {
  let index: Index<string, number>
  let createIndex: (name: string) => Index<string, number>

  beforeEach(() => {
    createIndex = (_name: string) => new Index()
    index = new Index()
  })

  describe('basic operations', () => {
    test('should add and get values', () => {
      index.addValue('key1', [10, 1])
      index.addValue('key1', [20, 2])

      const result = index.get('key1')
      expect(result).toEqual([
        [10, 1],
        [20, 2],
      ])
    })

    test('should return empty array for non-existent key', () => {
      const result = index.get('nonexistent')
      expect(result).toEqual([])
    })

    test('should handle zero multiplicities', () => {
      index.addValue('key1', [10, 1])
      index.addValue('key1', [10, -1])

      // Before compaction, values are stored as-is
      const result = index.get('key1')
      expect(result).toEqual([
        [10, 1],
        [10, -1],
      ])

      // After compaction, zero-multiplicity values are removed
      index.compact(['key1'])
      const compactedResult = index.get('key1')
      expect(compactedResult).toEqual([])
    })
  })

  describe('append', () => {
    test('should append data from another index', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 1])
      other.addValue('key1', [20, 1])
      other.addValue('key2', [30, 1])

      index.append(other)

      expect(index.get('key1')).toEqual([
        [10, 1],
        [20, 1],
      ])
      expect(index.get('key2')).toEqual([[30, 1]])
    })

    test('should combine multiplicities for same values', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 2])
      other.addValue('key1', [10, 3])

      index.append(other)

      // Before compaction, values are stored separately
      expect(index.get('key1')).toEqual([
        [10, 2],
        [10, 3],
      ])

      // After compaction, multiplicities are combined
      index.compact(['key1'])
      expect(index.get('key1')).toEqual([[10, 5]])
    })
  })

  describe('join', () => {
    test('should join two indexes', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 2])
      other.addValue('key1', [20, 3])

      const result = index.join(other)
      const entries = result.getInner()
      expect(entries).toHaveLength(1)
      expect(entries[0]).toEqual([['key1', [10, 20]], 6])
    })

    test('should return empty multiset when no matching keys', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 1])
      other.addValue('key2', [20, 1])

      const result = index.join(other)
      expect(result.getInner()).toEqual([])
    })

    test('should handle multiple values with same key', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 2])
      index.addValue('key1', [20, 3])
      other.addValue('key1', [30, 1])
      other.addValue('key1', [40, 2])

      const result = index.join(other)
      const entries = result.getInner()
      expect(entries).toHaveLength(4)
      expect(entries).toContainEqual([['key1', [10, 30]], 2])
      expect(entries).toContainEqual([['key1', [10, 40]], 4])
      expect(entries).toContainEqual([['key1', [20, 30]], 3])
      expect(entries).toContainEqual([['key1', [20, 40]], 6])
    })

    test('should handle multiple keys', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 2])
      index.addValue('key2', [20, 3])
      other.addValue('key1', [30, 1])
      other.addValue('key2', [40, 2])

      const result = index.join(other)
      const entries = result.getInner()
      expect(entries).toHaveLength(2)
      expect(entries).toContainEqual([['key1', [10, 30]], 2])
      expect(entries).toContainEqual([['key2', [20, 40]], 6])
    })

    test('should handle zero multiplicities correctly', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 0])
      other.addValue('key1', [20, 2])

      const result = index.join(other)
      expect(result.getInner()).toEqual([])
    })
  })

  test('should track and compact only changed keys', () => {
    // Add values to different keys
    index.addValue('key1', [10, 1])
    index.addValue('key1', [10, -1]) // This should cancel out when compacted
    index.addValue('key2', [20, 1])
    index.addValue('key3', [30, 1])

    // Create another index that we'll append from
    const other = createIndex('other')
    other.addValue('key4', [40, 1])
    other.addValue('key4', [40, -1]) // This should cancel out when compacted
    other.addValue('key5', [50, 1])

    // Append should also track changed keys
    index.append(other)

    // Before compaction, all values should be stored as-is
    expect(index.get('key1')).toEqual([
      [10, 1],
      [10, -1],
    ])
    expect(index.get('key2')).toEqual([[20, 1]])
    expect(index.get('key3')).toEqual([[30, 1]])
    expect(index.get('key4')).toEqual([
      [40, 1],
      [40, -1],
    ])
    expect(index.get('key5')).toEqual([[50, 1]])

    // Compact without arguments should only compact changed keys
    index.compact()

    // After compaction, values should be consolidated and zero-multiplicity entries removed
    expect(index.get('key1')).toEqual([]) // Cancelled out
    expect(index.get('key2')).toEqual([[20, 1]])
    expect(index.get('key3')).toEqual([[30, 1]])
    expect(index.get('key4')).toEqual([]) // Cancelled out
    expect(index.get('key5')).toEqual([[50, 1]])

    // Add more values after compaction
    index.addValue('key2', [25, 1])
    index.addValue('key6', [60, 1])

    // Only key2 and key6 should have new uncompacted values
    expect(index.get('key2')).toEqual([
      [20, 1],
      [25, 1],
    ])
    expect(index.get('key6')).toEqual([[60, 1]])

    // Compact again - should only affect key2 and key6
    index.compact()

    expect(index.get('key2')).toEqual([
      [20, 1],
      [25, 1],
    ]) // Both values preserved
    expect(index.get('key6')).toEqual([[60, 1]])

    // Other keys should remain unchanged
    expect(index.get('key3')).toEqual([[30, 1]])
    expect(index.get('key5')).toEqual([[50, 1]])
  })
})
