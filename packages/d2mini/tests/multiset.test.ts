import { describe, it, expect, beforeEach } from 'vitest'
import { MultiSet } from '../src/multiset.js'

const sortData = (a: any, b: any) => {
  return JSON.stringify(a[0]).localeCompare(JSON.stringify(b[0]))
}

describe('MultiSet', () => {
  describe('basic operations', () => {
    let a: MultiSet<[string, string | string[]]>
    let b: MultiSet<[string, string | string[]]>

    beforeEach(() => {
      a = new MultiSet<[string, string | string[]]>([
        [['apple', '$5'], 1],
        [['banana', '$2'], 1],
      ])
      b = new MultiSet<[string, string | string[]]>([
        [['apple', '$3'], 1],
        [['apple', '$5'], 1],
        [['apple', ['granny smith', '$2']], 1],
        [['kiwi', '$2'], 1],
      ])
    })

    it('should concatenate two multisets', () => {
      const concat = a.concat(b)
      const res = concat.getInner().sort(sortData)
      expect(res).toEqual([
        [['apple', '$3'], 1],
        [['apple', '$5'], 2],
        [['apple', ['granny smith', '$2']], 1],
        [['banana', '$2'], 1],
        [['kiwi', '$2'], 1],
      ])
    })

    it('should filter elements', () => {
      const filtered = a.filter((data) => data[0] !== 'apple')
      expect(filtered.getInner()).toEqual([[['banana', '$2'], 1]])
    })

    it('should map elements', () => {
      const mapped = a.map((data) => [data[1], data[0]])
      expect(mapped.getInner().sort(sortData)).toEqual([
        [['$2', 'banana'], 1],
        [['$5', 'apple'], 1],
      ])
    })
  })

  it('should handle negative multiplicities correctly', () => {
    const a = new MultiSet([[1, 1]])
    const b = new MultiSet([[1, -1]])
    const result = a.concat(b).consolidate()
    expect(result.getInner()).toHaveLength(0)
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

  it('should consolidate objects by reference', () => {
    const a = { a: 1 }
    const a2 = { a: 1 }
    const b = { b: 2 }

    const m1 = new MultiSet<Record<string, number>>([
      [a, 1],
      [a2, 1],
    ])
    const m2 = new MultiSet<Record<string, number>>([
      [a2, 1],
      [b, 1],
    ])
    const result = m1.concat(m2).consolidate()
    expect(result.getInner().length).toEqual(3)
    expect(result.get(a)).toEqual(1)
    expect(result.get(a2)).toEqual(2)
    expect(result.get(b)).toEqual(1)
  })
})
