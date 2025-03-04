import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { v, Antichain } from '../src/order.js'
import { Index } from '../src/version-index.js'
import { SQLIndex } from '../src/sqlite/version-index.js'
import Database from 'better-sqlite3'
import fs from 'fs'
import { BetterSQLite3Wrapper } from '../src/sqlite/database.js'

// Helper function to compare arrays
function arraysEqual(a: any[], b: any[]): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}

const SAVE_DB = true
const DB_FILENAME = 'test-version-index.db'

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
    let sqlite: Database.Database | null = null
    let db: BetterSQLite3Wrapper | null = null

    beforeEach(() => {
      if (name === 'in-memory') {
        db = null
        createIndex = (_name: string) => new Index() as T
      } else {
        if (SAVE_DB) {
          if (fs.existsSync(DB_FILENAME)) {
            fs.unlinkSync(DB_FILENAME)
          }
          sqlite = new Database(DB_FILENAME)
          db = new BetterSQLite3Wrapper(sqlite)
        } else {
          sqlite = new Database(':memory:')
          db = new BetterSQLite3Wrapper(sqlite)
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

      test('should handle multiple values with same key at different versions', () => {
        const version1 = v([1])
        const version2 = v([2])
        const other = createIndex('other')

        index.addValue('key1', version1, [10, 2])
        index.addValue('key1', version2, [20, 3])
        other.addValue('key1', version1, [30, 1])
        other.addValue('key1', version2, [40, 2])

        // @ts-expect-error
        const result = index.join(other)
        expect(result).toHaveLength(2)

        // Version 1 join results
        const [version1Result, multiset1] = result[0]
        expect(version1Result).toEqual(version1)
        const entries1 = multiset1.getInner()
        expect(entries1).toContainEqual([['key1', [10, 30]], 2])

        // Version 2 join results
        const [version2Result, multiset2] = result[1]
        expect(version2Result).toEqual(version2)
        const entries2 = multiset2.getInner()
        expect(entries2).toContainEqual([['key1', [20, 40]], 6])
      })

      test('should handle joins with multidimensional versions', () => {
        const version1 = v([1, 0])
        const version2 = v([1, 1])
        const version3 = v([2, 0])
        const other = createIndex('other')

        index.addValue('key1', version1, [10, 1])
        index.addValue('key1', version2, [20, 1])
        other.addValue('key1', version2, [30, 1])
        other.addValue('key1', version3, [40, 1])

        // @ts-expect-error
        const result = index.join(other).map(([v, m]) => [v, m.getInner()])
        expect(result).toEqual([
          [
            v([1, 1]),
            [
              [['key1', [10, 30]], 1],
              [['key1', [20, 30]], 1],
            ],
          ],
          [v([2, 0]), [[['key1', [10, 40]], 1]]],
          [v([2, 1]), [[['key1', [20, 40]], 1]]],
        ])
      })

      test('should handle multiple keys with overlapping versions', () => {
        const version = v([1])
        const other = createIndex('other')

        index.addValue('key1', version, [10, 2])
        index.addValue('key2', version, [20, 3])
        other.addValue('key1', version, [30, 1])
        other.addValue('key2', version, [40, 2])

        // @ts-expect-error
        const result = index.join(other)
        expect(result).toHaveLength(1)

        const [resultVersion, multiset] = result[0]
        expect(resultVersion).toEqual(version)

        const entries = multiset.getInner()
        expect(entries).toHaveLength(2)
        expect(entries).toContainEqual([['key1', [10, 30]], 2])
        expect(entries).toContainEqual([['key2', [20, 40]], 6])
      })

      test('should handle zero multiplicities correctly', () => {
        const version = v([1])
        const other = createIndex('other')

        index.addValue('key1', version, [10, 0])
        other.addValue('key1', version, [20, 2])

        // @ts-expect-error
        const result = index.join(other)
        expect([0, 1]).toContain(result.length)

        // The sqlite implementation doesn't return rows with multiplicity 0
        if (result.length === 1) {
          const [_, multiset] = result[0]
          const entries = multiset.getInner()
          expect(entries).toHaveLength(1)
          expect(entries[0][1]).toBe(0) // Multiplicity should be 0
        } else {
          expect(result).toEqual([])
        }
      })

      test('should handle complex version hierarchies', () => {
        const version1 = v([1, 0])
        const version2 = v([0, 1])
        const version3 = v([1, 1])
        const version4 = v([2, 1])
        const other = createIndex('other')

        // Add values at different versions in first index
        index.addValue('key1', version1, [10, 1])
        index.addValue('key1', version3, [20, 2])
        index.addValue('key1', version4, [30, 3])

        // Add values at different versions in second index
        other.addValue('key1', version2, [40, 1])
        other.addValue('key1', version2, [50, 2])
        other.addValue('key1', version4, [60, 3])

        // @ts-expect-error
        const result = index.join(other)
        const expected = [
          [
            v([1, 1]),
            [
              [['key1', [10, 40]], 1],
              [['key1', [10, 50]], 2],
              [['key1', [20, 40]], 2],
              [['key1', [20, 50]], 4],
            ],
          ],
          [
            v([2, 1]),
            [
              [['key1', [10, 60]], 3],
              [['key1', [20, 60]], 6],
              [['key1', [30, 40]], 3],
              [['key1', [30, 50]], 6],
              [['key1', [30, 60]], 9],
            ],
          ],
        ]
        expect(result.map(([v, m]) => [v, m.getInner()])).toEqual(expected)
      })

      test('joinAll should join multiple indexes with inner join', () => {
        const version = v([1])
        const other1 = createIndex('other1')
        const other2 = createIndex('other2')

        index.addValue('key1', version, [10, 2])
        other1.addValue('key1', version, [20, 3])
        other2.addValue('key1', version, [30, 4])

        // @ts-expect-error
        const result = index.joinAll([other1, other2])
        expect(result).toHaveLength(1)

        const [resultVersion, multiset] = result[0]
        expect(resultVersion).toEqual(version)

        // The join should produce [key1, [10, 20, 30]] with multiplicity 24 (2 * 3 * 4)
        const entries = multiset.getInner()
        expect(entries).toHaveLength(1)
        expect(entries[0]).toEqual([['key1', [10, 20, 30]], 24])
      })

      test('joinAll should return empty array when no matching keys with inner join', () => {
        const version = v([1])
        const other1 = createIndex('other1')
        const other2 = createIndex('other2')

        index.addValue('key1', version, [10, 1])
        other1.addValue('key2', version, [20, 1])
        other2.addValue('key1', version, [30, 1])

        // @ts-expect-error
        const result = index.joinAll([other1, other2])
        expect(result).toEqual([])
      })

      test('joinAll should include keys from base index only with left join', () => {
        const version = v([1])
        const other1 = createIndex('other1')
        const other2 = createIndex('other2')

        index.addValue('key1', version, [10, 2])
        index.addValue('key2', version, [15, 3])
        other1.addValue('key1', version, [20, 3])
        other2.addValue('key1', version, [30, 4])

        // @ts-expect-error
        const result = index.joinAll([other1, other2], 'left')
        expect(result).toHaveLength(1)

        const [resultVersion, multiset] = result[0]
        expect(resultVersion).toEqual(version)

        // Should have key1 with all values and key2 with nulls for other indexes
        const entries = multiset.getInner()
        expect(entries).toHaveLength(2)
        expect(entries).toContainEqual([['key1', [10, 20, 30]], 24])
        expect(entries).toContainEqual([['key2', [15, null, null]], 3])
      })

      test('joinAll should handle multiple values at different versions', () => {
        const version1 = v([1])
        const version2 = v([2])
        const other1 = createIndex('other1')
        const other2 = createIndex('other2')

        index.addValue('key1', version1, [10, 1])
        index.addValue('key1', version2, [15, 2])
        other1.addValue('key1', version1, [20, 1])
        other1.addValue('key1', version2, [25, 2])
        other2.addValue('key1', version1, [30, 1])
        other2.addValue('key1', version2, [35, 2])

        // @ts-expect-error
        const result = index.joinAll([other1, other2])
        expect(result).toHaveLength(2)

        // Check version1 results
        const v1Results = result.find(([v]) => v.equals(version1))
        expect(v1Results).toBeDefined()
        const [v1, m1] = v1Results!
        expect(v1.equals(version1)).toBe(true)

        // Check version2 results
        const v2Results = result.find(([v]) => v.equals(version2))
        expect(v2Results).toBeDefined()
        const [v2, m2] = v2Results!
        expect(v2.equals(version2)).toBe(true)

        // Just check that the entries contain the expected data and multiplicity
        const entries1 = m1.getInner()
        expect(
          entries1.some(
            (e) =>
              e[0][0] === 'key1' &&
              arraysEqual(e[0][1], [10, 20, 30]) &&
              e[1] === 1,
          ),
        ).toBe(true)

        const entries2 = m2.getInner()
        expect(
          entries2.some(
            (e) =>
              e[0][0] === 'key1' &&
              arraysEqual(e[0][1], [15, 25, 35]) &&
              e[1] === 8,
          ),
        ).toBe(true)
      })

      test('joinAll should handle empty other indexes array', () => {
        const version = v([1])
        index.addValue('key1', version, [10, 2])

        const result = index.joinAll([])
        expect(result).toHaveLength(1)

        const [resultVersion, multiset] = result[0]
        expect(resultVersion).toEqual(version)

        const entries = multiset.getInner()
        expect(entries).toHaveLength(1)
        expect(entries[0]).toEqual([['key1', [10]], 2])
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

      test('should append after compact', () => {
        const version1 = v([1])
        const version2 = v([2])
        const version3 = v([3])
        const frontier = new Antichain([version2])

        index.addValue('key1', version1, [10, 1])

        expect(index.reconstructAt('key1', version1)).toEqual([[10, 1]])

        index.compact(frontier)

        const other = createIndex('other')
        other.addValue('key1', version2, [20, 1])

        // @ts-expect-error
        index.append(other)

        const other2 = createIndex('other2')
        other2.addValue('key1', version3, [30, 1])

        // @ts-expect-error
        index.append(other2)

        expect(index.reconstructAt('key1', version3)).toEqual([
          [10, 1],
          [20, 1],
          [30, 1],
        ])
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
      if (sqlite) {
        sqlite.close()
      }
    })
  })
}

// Run tests for both implementations
createIndexTests('in-memory')
createIndexTests('sqlite')
