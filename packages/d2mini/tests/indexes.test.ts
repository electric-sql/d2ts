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
    test('should add and reconstruct values', () => {
      index.addValue('key1', [10, 1])
      index.addValue('key1', [20, 2])

      const result = index.reconstruct('key1')
      expect(result).toEqual([
        [10, 1],
        [20, 2],
      ])
    })

    test('should return empty array for non-existent key', () => {
      const result = index.reconstruct('nonexistent')
      expect(result).toEqual([])
    })

    test('should handle zero multiplicities', () => {
      index.addValue('key1', [10, 1])
      index.addValue('key1', [10, -1])

      const result = index.reconstruct('key1')
      expect(result).toEqual([])
    })
  })

  describe('append', () => {
    test('should append data from another index', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 1])
      other.addValue('key1', [20, 1])
      other.addValue('key2', [30, 1])

      index.append(other)

      expect(index.reconstruct('key1')).toEqual([
        [10, 1],
        [20, 1],
      ])
      expect(index.reconstruct('key2')).toEqual([[30, 1]])
    })

    test('should combine multiplicities for same values', () => {
      const other = createIndex('other')

      index.addValue('key1', [10, 2])
      other.addValue('key1', [10, 3])

      index.append(other)

      expect(index.reconstruct('key1')).toEqual([[10, 5]])
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
})
