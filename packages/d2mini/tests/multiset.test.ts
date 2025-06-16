import { describe, it, expect, beforeEach } from 'vitest'
import { MultiSet } from '../src/multiset.js'

describe('MultiSet', () => {
  describe('basic operations', () => {
    let a: MultiSet<[string, string | string[]]>
    let b: MultiSet<[string, string | string[]]>

    beforeEach(() => {
      a = new MultiSet<[string, string | string[]]>([
        [['apple', '$5'], 2],
        [['banana', '$2'], 1],
      ])
      b = new MultiSet<[string, string | string[]]>([
        [['apple', '$3'], 1],
        [['apple', ['granny smith', '$2']], 1],
        [['kiwi', '$2'], 1],
      ])
    })

    it('should concatenate two multisets', () => {
      const concat = a.concat(b)
      expect(concat.getInner()).toEqual([
        [['apple', '$5'], 2],
        [['banana', '$2'], 1],
        [['apple', '$3'], 1],
        [['apple', ['granny smith', '$2']], 1],
        [['kiwi', '$2'], 1],
      ])
    })

    it('should join two multisets', () => {
      const joined = a.join(b)
      expect(joined.getInner()).toEqual([
        [['apple', ['$5', '$3']], 2],
        [['apple', ['$5', ['granny smith', '$2']]], 2],
      ])
    })

    it('should filter elements', () => {
      const filtered = a.filter((data) => data[0] !== 'apple')
      expect(filtered.getInner()).toEqual([[['banana', '$2'], 1]])
    })

    it('should map elements', () => {
      const mapped = a.map((data) => [data[1], data[0]])
      expect(mapped.getInner()).toEqual([
        [['$5', 'apple'], 2],
        [['$2', 'banana'], 1],
      ])
    })
  })

  describe('numeric operations', () => {
    let d: MultiSet<[string, number]>

    beforeEach(() => {
      d = new MultiSet([
        [['apple', 11], 1],
        [['apple', 3], 2],
        [['banana', 2], 3],
        [['coconut', 3], 1],
      ])
    })

    it('should calculate sum', () => {
      const sum = d.sum()
      expect(sum.getInner()).toEqual([
        [['apple', 17], 1],
        [['banana', 6], 1],
        [['coconut', 3], 1],
      ])
    })

    it('should calculate count', () => {
      const count = d.count()
      expect(count.getInner()).toEqual([
        [['apple', 3], 1],
        [['banana', 3], 1],
        [['coconut', 1], 1],
      ])
    })
  })

  it('should handle min/max operations', () => {
    const c = new MultiSet([
      [['apple', '$5'], 2],
      [['banana', '$2'], 1],
      [['apple', '$2'], 20],
    ])

    // Test min
    const min = c.min()
    expect(min.getInner()).toEqual([
      [['apple', '$2'], 1],
      [['banana', '$2'], 1],
    ])

    // Test max
    const max = c.max()
    expect(max.getInner()).toEqual([
      [['apple', '$5'], 1],
      [['banana', '$2'], 1],
    ])
  })

  it('should handle negative multiplicities correctly', () => {
    const a = new MultiSet([[1, 1]])
    const b = new MultiSet([[1, -1]])
    const result = a.concat(b).consolidate()
    expect(result.getInner()).toHaveLength(0)
  })

  it('should throw on invalid operations', () => {
    const a = new MultiSet([[1, -1]])
    expect(() => a.min()).toThrow()
    expect(() => a.max()).toThrow()
    expect(() => a.distinct()).toThrow()
  })

  it('should consolidate simple string values', () => {
    // Create a multiset with duplicate string values that need consolidation
    const stringSet = new MultiSet<string>([
      ['a', 1],
      ['a', 2],
      ['b', 3],
      ['b', 1],
      ['c', 1],
    ])

    const consolidated = stringSet.consolidate()

    // After consolidation, duplicates should be combined
    expect(consolidated.getInner()).toEqual([
      ['a', 3],
      ['b', 4],
      ['c', 1],
    ])
  })

  it('should consolidate simple number values', () => {
    // Create a multiset with duplicate number values that need consolidation
    const numberSet = new MultiSet<number>([
      [1, 2],
      [1, 3],
      [2, 1],
      [2, 2],
      [3, 1],
    ])

    const consolidated = numberSet.consolidate()

    // After consolidation, duplicates should be combined
    expect(consolidated.getInner()).toEqual([
      [1, 5],
      [2, 3],
      [3, 1],
    ])
  })

  it('should consolidate mixed string and number values', () => {
    // Create a multiset with mixed types that will require JSON stringification
    const mixedSet = new MultiSet<string | number>([
      [1, 2],
      ['1', 3],
      [1, 1],
      ['1', 2],
    ])

    const consolidated = mixedSet.consolidate()

    // After consolidation, the number 1 and string '1' should remain separate
    expect(consolidated.getInner()).toEqual([
      [1, 3],
      ['1', 5],
    ])
  })
})
