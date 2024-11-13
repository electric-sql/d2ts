import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { v, Antichain } from '../src/order'
import { Index } from '../src/version-index'
import { SQLIndex } from '../src/version-index-sqlite'
import Database from 'better-sqlite3'
import fs from 'fs'

const SAVE_DB = true

// Test factory that runs the same tests for both implementations
function createIndexTests<
  N extends 'in-memory' | 'sqlite',
  T extends N extends 'in-memory'
    ? Index<string, number>
    : SQLIndex<string, number>,
>(name: N) {
  describe(name, () => {
    let index: T
    let createIndex: (name: string) => T
    let db: InstanceType<typeof Database> | null = null

    beforeEach(() => {
      if (name === 'in-memory') {
        db = null
        createIndex = (_name: string) => new Index() as T
      } else {
        if (SAVE_DB) {
          if (fs.existsSync('./test.db')) {
            fs.unlinkSync('./test.db')
          }
          db = new Database('./test.db')
        } else {
          db = new Database(':memory:')
        }
        createIndex = (name: string) => new SQLIndex(db!, name) as T
      }

      index = createIndex('main')
    })

    describe('basic operations', () => {
      test('should add and reconstruct values', () => {
        const version = v([1])
        index.addValue('key1', version, [10, 1])
        index.addValue('key1', version, [20, 2])

        const result = index.reconstructAt('key1', version)
        expect(result).toEqual([
          [10, 1],
          [20, 2],
        ])
      })

      test('should return empty array for non-existent key', () => {
        const version = v([1])
        const result = index.reconstructAt('nonexistent', version)
        expect(result).toEqual([])
      })

      test('should return versions for a key', () => {
        const version1 = v([1])
        const version2 = v([2])

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
        const version = v([1])
        const other = createIndex('other')

        index.addValue('key1', version, [10, 1])
        other.addValue('key1', version, [20, 1])
        other.addValue('key2', version, [30, 1])

        // @ts-expect-error
        index.append(other)

        expect(index.reconstructAt('key1', version)).toEqual([
          [10, 1],
          [20, 1],
        ])
        expect(index.reconstructAt('key2', version)).toEqual([[30, 1]])
      })
    })

    describe('join', () => {
      test('should join two indexes', () => {
        const version = v([1])
        const other = createIndex('other')

        index.addValue('key1', version, [10, 2])
        other.addValue('key1', version, [20, 3])

        // @ts-expect-error
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
        const version = v([1])
        const other = createIndex('other')

        index.addValue('key1', version, [10, 1])
        other.addValue('key2', version, [20, 1])

        // @ts-expect-error
        const result = index.join(other)
        expect(result).toEqual([])
      })
    })

    describe('compact', () => {
      test('should compact versions according to frontier', () => {
        const version1 = v([1])
        const version2 = v([2])
        const frontier = new Antichain([v([2])])

        index.addValue('key1', version1, [10, 1])
        index.addValue('key1', version1, [10, 2])
        index.addValue('key1', version2, [10, -1])

        index.compact(frontier)

        const result = index.reconstructAt('key1', version2)
        expect(result).toEqual([[10, 2]])
      })

      test('should compact multiple keys correctly', () => {
        const version1 = v([1])
        const version2 = v([2])
        const frontier = new Antichain([v([2])])

        index.addValue('key1', version1, [10, 3])
        index.addValue('key2', version1, [20, 2])
        index.addValue('key1', version2, [10, -1])
        index.addValue('key2', version2, [20, 3])

        index.compact(frontier)

        expect(index.reconstructAt('key1', version2)).toEqual([[10, 2]])
        expect(index.reconstructAt('key2', version2)).toEqual([[20, 5]])
      })

      test('should handle multiple values for same key and version', () => {
        const version1 = v([1])
        const frontier = new Antichain([v([2])])

        index.addValue('key1', version1, [10, 1])
        index.addValue('key1', version1, [20, 2])
        index.addValue('key1', version1, [10, 3])

        index.compact(frontier)

        const result = index.reconstructAt('key1', v([2]))
        expect(result).toEqual([
          [10, 4],
          [20, 2],
        ])
      })

      test('should handle compaction with multidimensional versions', () => {
        const version1 = v([1, 0])
        const version2 = v([0, 1])
        const version3 = v([1, 1])
        const frontier = new Antichain([v([1, 1])])

        index.addValue('key1', version1, [10, 1])
        index.addValue('key1', version2, [10, 2])
        index.addValue('key1', version3, [10, -1])

        index.compact(frontier)

        const result = index.reconstructAt('key1', version3)
        expect(result).toEqual([[10, 2]])
      })

      test('should handle selective key compaction', () => {
        const version1 = v([1])
        const version2 = v([2])
        const frontier = new Antichain([v([2])])

        index.addValue('key1', version1, [10, 3])
        index.addValue('key2', version1, [20, 2])
        index.addValue('key1', version2, [10, -1])
        index.addValue('key2', version2, [20, 3])

        // Only compact 'key1'
        index.compact(frontier, ['key1'])

        // key1 should be compacted
        expect(index.reconstructAt('key1', version2)).toEqual([[10, 2]])
        
        // key2 should maintain original versions
        const key2Versions = index.versions('key2')
        expect(key2Versions).toHaveLength(2)
        expect(key2Versions).toContainEqual(version1)
        expect(key2Versions).toContainEqual(version2)
      })

      test('should throw error for invalid compaction frontier', () => {
        const version = v([1])
        const frontier1 = new Antichain([v([2])])
        const frontier2 = new Antichain([v([1])])

        index.addValue('key1', version, [10, 1])
        index.compact(frontier1)

        expect(() => {
          index.compact(frontier2)
        }).toThrow('Invalid compaction frontier')
      })
    })

    describe('validation', () => {
      test('should throw error for invalid version access', () => {
        const version1 = v([1])
        const frontier = new Antichain([v([2])])

        index.addValue('key1', version1, [10, 1])
        index.compact(frontier)

        expect(() => {
          index.reconstructAt('key1', version1)
        }).toThrow('Invalid version')
      })
    })

    // Clean up resources if needed (especially for SQLite)
    afterEach(async () => {
      if ('close' in index) {
        // @ts-expect-error
        await (index as SQLIndex<string, number>).close()
        db?.close()
      }
    })
  })
}

// Run tests for both implementations
createIndexTests('in-memory')
createIndexTests('sqlite')
