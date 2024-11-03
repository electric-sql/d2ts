import { describe, it, expect } from 'vitest'
import { MultiSet } from '../src/multiset'

describe('MultiSet', () => {
  it('should handle basic operations', () => {
    const a = new MultiSet<[string, string | string[]]>([
      [['apple', '$5'], 2],
      [['banana', '$2'], 1],
    ])
    const b = new MultiSet<[string, string | string[]]>([
      [['apple', '$3'], 1],
      [['apple', ['granny smith', '$2']], 1],
      [['kiwi', '$2'], 1],
    ])

    // Test concat
    const concat = a.concat(b)
    expect(concat.getInner()).toEqual([
      [['apple', '$5'], 2],
      [['banana', '$2'], 1],
      [['apple', '$3'], 1],
      [['apple', ['granny smith', '$2']], 1],
      [['kiwi', '$2'], 1],
    ])

    // Test join
    const joined = a.join(b)
    expect(joined.getInner()).toEqual([
      [['apple', ['$5', '$3']], 2],
      [['apple', ['$5', ['granny smith', '$2']]], 2],
    ])

    // Test filter
    const filtered = a.filter((data) => data[0] !== 'apple')
    expect(filtered.getInner()).toEqual([[['banana', '$2'], 1]])

    // Test map
    const mapped = a.map((data) => [data[1], data[0]])
    expect(mapped.getInner()).toEqual([
      [['$5', 'apple'], 2],
      [['$2', 'banana'], 1],
    ])
  })

  it('should handle numeric operations', () => {
    const d = new MultiSet([
      [['apple', 11], 1],
      [['apple', 3], 2],
      [['banana', 2], 3],
      [['coconut', 3], 1],
    ])

    // Test sum
    const sum = d.sum()
    expect(sum.getInner()).toEqual([
      [['apple', 17], 1],
      [['banana', 6], 1],
      [['coconut', 3], 1],
    ])

    // Test count
    const count = d.count()
    expect(count.getInner()).toEqual([
      [['apple', 3], 1],
      [['banana', 3], 1],
      [['coconut', 1], 1],
    ])
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

  it('should handle iteration', () => {
    const e = new MultiSet([[1, 1]])

    const addOne = (collection: MultiSet<number>) => {
      return collection
        .map((data) => data + 1)
        .concat(collection)
        .filter((data) => data <= 5)
        .map((data) => [data, []])
        .distinct()
        .map((data) => data[0])
        .consolidate()
    }

    // @ts-ignore
    const result = e.iterate(addOne).map((data) => [data, data * data])
    expect(result.getInner()).toEqual([
      [[1, 1], 1],
      [[2, 4], 1],
      [[3, 9], 1],
      [[4, 16], 1],
      [[5, 25], 1],
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
})
