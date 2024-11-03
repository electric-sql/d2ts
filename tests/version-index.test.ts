import { describe, test, expect, beforeEach } from 'vitest'
import { Version, Antichain } from '../src/order'
import { Index } from '../src/version-index'

describe('Index', () => {
  let index: Index<string, number>

  beforeEach(() => {
    index = new Index()
  })

  describe('basic operations', () => {
    test('should add and reconstruct values', () => {
      const version = new Version([1])
      index.addValue('key1', version, [10, 1])
      index.addValue('key1', version, [20, 2])

      const result = index.reconstructAt('key1', version)
      expect(result).toEqual([[10, 1], [20, 2]])
    })

    test('should return empty array for non-existent key', () => {
      const version = new Version([1])
      const result = index.reconstructAt('nonexistent', version)
      expect(result).toEqual([])
    })

    test('should return versions for a key', () => {
      const version1 = new Version([1])
      const version2 = new Version([2])
      
      index.addValue('key1', version1, [10, 1])
      index.addValue('key1', version2, [20, 1])

      const versions = index.versions('key1')
      expect(versions).toHaveLength(2)
      expect(versions).toContainEqual(version1)
      expect(versions).toContainEqual(version2)
    })
  })

  describe('append', () => {
    test('should append data from another index', () => {
      const version = new Version([1])
      const other = new Index<string, number>()

      index.addValue('key1', version, [10, 1])
      other.addValue('key1', version, [20, 1])
      other.addValue('key2', version, [30, 1])

      index.append(other)

      expect(index.reconstructAt('key1', version)).toEqual([[10, 1], [20, 1]])
      expect(index.reconstructAt('key2', version)).toEqual([[30, 1]])
    })
  })

  describe('join', () => {
    test('should join two indexes', () => {
      const version = new Version([1])
      const other = new Index<string, number>()

      index.addValue('key1', version, [10, 2])
      other.addValue('key1', version, [20, 3])

      const result = index.join(other)
      expect(result).toHaveLength(1)
      
      const [resultVersion, multiset] = result[0]
      expect(resultVersion).toEqual(version)
      
      // The join should produce [key1, [10, 20]] with multiplicity 6 (2 * 3)
      const entries = multiset.getInner()
      expect(entries).toHaveLength(1)
      expect(entries[0]).toEqual([['key1', [10, 20]], 6])
    })

    test('should return empty array when no matching keys', () => {
      const version = new Version([1])
      const other = new Index<string, number>()

      index.addValue('key1', version, [10, 1])
      other.addValue('key2', version, [20, 1])

      const result = index.join(other)
      expect(result).toEqual([])
    })
  })

  describe('compact', () => {
    test('should compact versions according to frontier', () => {
      const version1 = new Version([1])
      const version2 = new Version([2])
      const frontier = new Antichain([new Version([2])])

      index.addValue('key1', version1, [10, 1])
      index.addValue('key1', version1, [10, 2])
      index.addValue('key1', version2, [10, -1])

      index.compact(frontier)

      const result = index.reconstructAt('key1', version2)
      expect(result).toEqual([[10, -1], [10, 3]])
    })

    test('should throw error for invalid compaction frontier', () => {
      const version = new Version([1])
      const frontier1 = new Antichain([new Version([2])])
      const frontier2 = new Antichain([new Version([1])])

      index.addValue('key1', version, [10, 1])
      index.compact(frontier1)

      expect(() => {
        index.compact(frontier2)
      }).toThrow('Invalid compaction frontier')
    })
  })

  describe('validation', () => {
    test('should throw error for invalid version access', () => {
      const version1 = new Version([1])
      const frontier = new Antichain([new Version([2])])

      index.addValue('key1', version1, [10, 1])
      index.compact(frontier)

      expect(() => {
        index.reconstructAt('key1', version1)
      }).toThrow('Invalid version')
    })
  })
}) 