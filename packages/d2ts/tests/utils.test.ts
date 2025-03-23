import { describe, it, expect } from 'vitest'
import { DefaultMap, WeakRefMap, SortedSet, SortedMap } from '../src/utils.js'

describe('DefaultMap', () => {
  it('should return default value for missing keys', () => {
    const map = new DefaultMap(() => 0)
    expect(map.get('missing')).toBe(0)
  })

  it('should store and retrieve values', () => {
    const map = new DefaultMap(() => 0)
    map.set('key', 42)
    expect(map.get('key')).toBe(42)
  })

  it('should accept initial entries', () => {
    const map = new DefaultMap(() => 0, [['key', 1]])
    expect(map.get('key')).toBe(1)
  })

  it('should update values using the update method', () => {
    const map = new DefaultMap(() => 0)
    map.update('key', (value) => value + 1)
    expect(map.get('key')).toBe(1)

    map.update('key', (value) => value * 2)
    expect(map.get('key')).toBe(2)
  })
})

describe('WeakRefMap', () => {
  it('should return default value for missing keys', () => {
    const map = new WeakRefMap<string, object>()
    expect(map.get('missing')).toBeNull()
  })

  it('should store and retrieve values', () => {
    const map = new WeakRefMap<string, object>()
    const obj = { test: 'value' }
    map.set('key', obj)
    expect(map.get('key')).toBe(obj)
  })

  it('should accept initial entries', () => {
    const map = new WeakRefMap<string, object>()
    const obj = { test: 'value' }
    map.set('key', obj)
    expect(map.get('key')).toBe(obj)
  })

  // it('should cleanup dereferenced objects after garbage collection', async () => {
  //   const map = new WeakRefMap<string, object>()

  //   // Create object in a scope that will end
  //   {
  //     const obj = { test: 'value' }
  //     map.set('key', obj)
  //     expect(map.get('key')).toBe(obj)
  //   }

  //   // Force garbage collection if possible
  //   // Note: This is environment-specific and may not work everywhere
  //   // with node use --expose-gc to enable the `global.gc()` method
  //   if (global.gc) {
  //     // Run GC multiple times to ensure cleanup
  //     for (let i = 0; i < 3; i++) {
  //       global.gc()
  //       // Give finalizers a chance to run
  //       await new Promise(resolve => setTimeout(resolve, 0))
  //     }

  //     // Object should be gone now
  //     expect(map.get('key')).toBeNull()
  //   } else {
  //     console.warn('Test skipped: garbage collection not exposed. Run Node.js with --expose-gc flag.')
  //   }
  // })
})

describe('SortedSet', () => {
  it('should maintain sorted order of values', () => {
    const set = new SortedSet<number>([3, 1, 4, 2], (a, b) => a - b)
    expect(Array.from(set)).toEqual([1, 2, 3, 4])
  })

  it('should work with default comparison', () => {
    const set = new SortedSet<number>([3, 1, 4, 2])
    // Default sort converts to strings, so 1, 2, 3, 4 is expected
    expect(Array.from(set)).toEqual([1, 2, 3, 4])
  })

  it('should maintain sorted order when adding values', () => {
    const set = new SortedSet<number>([], (a, b) => a - b)
    set.add(3)
    set.add(1)
    set.add(4)
    set.add(2)
    expect(Array.from(set)).toEqual([1, 2, 3, 4])
  })

  it('should not add duplicate values', () => {
    const set = new SortedSet<number>([1, 2, 3], (a, b) => a - b)
    set.add(2)
    expect(Array.from(set)).toEqual([1, 2, 3])
    expect(set.size).toBe(3)
  })

  it('should return values as array with asArray()', () => {
    const set = new SortedSet<number>([3, 1, 4, 2], (a, b) => a - b)
    expect(set.asArray()).toEqual([1, 2, 3, 4])
  })

  it('should clear all values', () => {
    const set = new SortedSet<number>([1, 2, 3], (a, b) => a - b)
    set.clear()
    expect(set.size).toBe(0)
    expect(Array.from(set)).toEqual([])
  })

  it('should delete values', () => {
    const set = new SortedSet<number>([1, 2, 3, 4], (a, b) => a - b)
    expect(set.delete(2)).toBe(true)
    expect(Array.from(set)).toEqual([1, 3, 4])
    expect(set.delete(5)).toBe(false)
  })

  it('should iterate entries in sorted order', () => {
    const set = new SortedSet<number>([3, 1, 4, 2], (a, b) => a - b)
    const entries = Array.from(set.entries())
    expect(entries).toEqual([
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 4],
    ])
  })

  it('should find index of value with predicate', () => {
    const set = new SortedSet<number>([1, 2, 3, 4, 5], (a, b) => a - b)
    expect(set.findIndex((v) => v > 3)).toBe(3)
    expect(set.findIndex((v) => v === 2)).toBe(1)
    expect(set.findIndex((v) => v > 10)).toBe(-1)
  })

  it('should find last index of value with predicate', () => {
    const set = new SortedSet<number>([1, 2, 3, 4, 5], (a, b) => a - b)
    expect(set.findLastIndex((v) => v < 4)).toBe(2)
    expect(set.findLastIndex((v) => v === 5)).toBe(4)
    expect(set.findLastIndex((v) => v < 0)).toBe(-1)
  })

  it('should execute forEach in sorted order', () => {
    const set = new SortedSet<number>([3, 1, 4, 2], (a, b) => a - b)
    const result: number[] = []
    set.forEach((value) => {
      result.push(value)
    })
    expect(result).toEqual([1, 2, 3, 4])
  })

  it('should find index of value', () => {
    const set = new SortedSet<number>([1, 2, 3, 4, 5], (a, b) => a - b)
    expect(set.indexOf(3)).toBe(2)
    expect(set.indexOf(6)).toBe(-1)
  })

  it('should iterate keys in sorted order', () => {
    const set = new SortedSet<number>([3, 1, 4, 2], (a, b) => a - b)
    const keys = Array.from(set.keys())
    expect(keys).toEqual([1, 2, 3, 4])
  })

  it('should find last index of value', () => {
    const set = new SortedSet<number>([1, 2, 3, 3, 4, 5], (a, b) => a - b)
    expect(set.lastIndexOf(3)).toBe(2) // Only one 3 should be in the set
    expect(set.lastIndexOf(6)).toBe(-1)
  })

  it('should pop the last value', () => {
    const set = new SortedSet<number>([1, 2, 3, 4], (a, b) => a - b)
    expect(set.pop()).toBe(4)
    expect(Array.from(set)).toEqual([1, 2, 3])
    expect(set.size).toBe(3)
  })

  it('should shift the first value', () => {
    const set = new SortedSet<number>([1, 2, 3, 4], (a, b) => a - b)
    expect(set.shift()).toBe(1)
    expect(Array.from(set)).toEqual([2, 3, 4])
    expect(set.size).toBe(3)
  })

  it('should slice a range of values', () => {
    const set = new SortedSet<number>([1, 2, 3, 4, 5], (a, b) => a - b)
    const sliced = set.slice(1, 4)
    expect(Array.from(sliced)).toEqual([2, 3, 4])
    expect(sliced.size).toBe(3)
  })

  it('should iterate values in sorted order', () => {
    const set = new SortedSet<number>([3, 1, 4, 2], (a, b) => a - b)
    const values = Array.from(set.values())
    expect(values).toEqual([1, 2, 3, 4])
  })

  it('should get value at index', () => {
    const set = new SortedSet<number>([1, 2, 3, 4, 5], (a, b) => a - b)
    expect(set.valueAt(2)).toBe(3)
  })

  it('should work with custom objects', () => {
    type Person = { name: string; age: number }
    const people: Person[] = [
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ]

    const set = new SortedSet<Person>(people, (a, b) => a.age - b.age)
    const sorted = set.asArray()

    expect(sorted[0].name).toBe('Bob')
    expect(sorted[1].name).toBe('Alice')
    expect(sorted[2].name).toBe('Charlie')
  })

  it('should work with Symbol.iterator', () => {
    const set = new SortedSet<number>([3, 1, 4, 2], (a, b) => a - b)
    const result = [...set]
    expect(result).toEqual([1, 2, 3, 4])
  })
})

describe('SortedMap', () => {
  it('should maintain sorted order of keys', () => {
    const map = new SortedMap<number, string>(
      [
        [3, 'three'],
        [1, 'one'],
        [4, 'four'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    expect(Array.from(map.keys())).toEqual([1, 2, 3, 4])
  })

  it('should work with default comparison', () => {
    const map = new SortedMap<number, string>([
      [3, 'three'],
      [1, 'one'],
      [4, 'four'],
      [2, 'two'],
    ])
    // Default sort converts to strings, so 1, 2, 3, 4 is expected
    expect(Array.from(map.keys())).toEqual([1, 2, 3, 4])
  })

  it('should maintain sorted order when adding values', () => {
    const map = new SortedMap<number, string>([], (a, b) => a - b)
    map.set(3, 'three')
    map.set(1, 'one')
    map.set(4, 'four')
    map.set(2, 'two')
    expect(Array.from(map.keys())).toEqual([1, 2, 3, 4])
  })

  it('should update values for existing keys', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    map.set(1, 'ONE')
    expect(map.get(1)).toBe('ONE')
    expect(Array.from(map.keys())).toEqual([1, 2])
  })

  it('should convert to a regular Map with asMap()', () => {
    const sortedMap = new SortedMap<number, string>(
      [
        [3, 'three'],
        [1, 'one'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    const regularMap = sortedMap.asMap()

    expect(regularMap).toBeInstanceOf(Map)
    expect(regularMap).not.toBeInstanceOf(SortedMap)
    expect(regularMap.get(1)).toBe('one')
    expect(regularMap.get(2)).toBe('two')
    expect(regularMap.get(3)).toBe('three')
  })

  it('should clear all entries', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
      ],
      (a, b) => a - b,
    )
    map.clear()
    expect(map.size).toBe(0)
    expect(Array.from(map.entries())).toEqual([])
  })

  it('should delete entries', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
      ],
      (a, b) => a - b,
    )
    expect(map.delete(2)).toBe(true)
    expect(Array.from(map.keys())).toEqual([1, 3, 4])
    expect(map.delete(5)).toBe(false)
  })

  it('should iterate entries in sorted order', () => {
    const map = new SortedMap<number, string>(
      [
        [3, 'three'],
        [1, 'one'],
        [4, 'four'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    const entries = Array.from(map.entries())
    expect(entries).toEqual([
      [1, 'one'],
      [2, 'two'],
      [3, 'three'],
      [4, 'four'],
    ])
  })

  it('should find index of key with predicate', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    expect(map.findIndex((k) => k > 3)).toBe(3)
    expect(map.findIndex((k) => k === 2)).toBe(1)
    expect(map.findIndex((k) => k > 10)).toBe(-1)
  })

  it('should find index of value with predicate', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    expect(map.findIndexValue((v) => v.startsWith('f'))).toBe(3) // 'four' is at index 3
    expect(map.findIndexValue((v) => v === 'two')).toBe(1)
    expect(map.findIndexValue((v) => v === 'six')).toBe(-1)
  })

  it('should find last index of key with predicate', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    expect(map.findLastIndex((k) => k < 4)).toBe(2)
    expect(map.findLastIndex((k) => k === 5)).toBe(4)
    expect(map.findLastIndex((k) => k < 0)).toBe(-1)
  })

  it('should find last index of value with predicate', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    expect(map.findLastIndexValue((v) => v.includes('e'))).toBe(4) // 'five' is the last with 'e'
    expect(map.findLastIndexValue((v) => v === 'one')).toBe(0)
    expect(map.findLastIndexValue((v) => v === 'six')).toBe(-1)
  })

  it('should execute forEach in sorted order', () => {
    const map = new SortedMap<number, string>(
      [
        [3, 'three'],
        [1, 'one'],
        [4, 'four'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    const result: Array<[number, string]> = []
    map.forEach((value, key) => {
      result.push([key, value])
    })
    expect(result).toEqual([
      [1, 'one'],
      [2, 'two'],
      [3, 'three'],
      [4, 'four'],
    ])
  })

  it('should find index of key', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    expect(map.indexOf(3)).toBe(2)
    expect(map.indexOf(6)).toBe(-1)
  })

  it('should find index of value', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    expect(map.indexOfValue('three')).toBe(2)
    expect(map.indexOfValue('six')).toBe(-1)
  })

  it('should iterate keys in sorted order', () => {
    const map = new SortedMap<number, string>(
      [
        [3, 'three'],
        [1, 'one'],
        [4, 'four'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    const keys = Array.from(map.keys())
    expect(keys).toEqual([1, 2, 3, 4])
  })

  it('should find last index of key', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    expect(map.lastIndexOf(3)).toBe(2)
    expect(map.lastIndexOf(6)).toBe(-1)
  })

  it('should find last index of value', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    // Add a duplicate value to test lastIndexOfValue
    map.set(6, 'three')
    expect(map.lastIndexOfValue('three')).toBe(5) // Index of key 6
    expect(map.lastIndexOfValue('six')).toBe(-1)
  })

  it('should pop the last entry', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
      ],
      (a, b) => a - b,
    )
    expect(map.pop()).toEqual([4, 'four'])
    expect(map.size).toBe(3)
    expect(Array.from(map.keys())).toEqual([1, 2, 3])
  })

  it('should throw when popping from an empty map', () => {
    const map = new SortedMap<number, string>([], (a, b) => a - b)
    expect(() => map.pop()).toThrow('SortedMap is empty')
  })

  it('should shift the first entry', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
      ],
      (a, b) => a - b,
    )
    expect(map.shift()).toEqual([1, 'one'])
    expect(map.size).toBe(3)
    expect(Array.from(map.keys())).toEqual([2, 3, 4])
  })

  it('should throw when shifting from an empty map', () => {
    const map = new SortedMap<number, string>([], (a, b) => a - b)
    expect(() => map.shift()).toThrow('SortedMap is empty')
  })

  it('should slice a range of entries', () => {
    const map = new SortedMap<number, string>(
      [
        [1, 'one'],
        [2, 'two'],
        [3, 'three'],
        [4, 'four'],
        [5, 'five'],
      ],
      (a, b) => a - b,
    )
    const sliced = map.slice(1, 4)
    expect(sliced.size).toBe(3)
    expect(Array.from(sliced.entries())).toEqual([
      [2, 'two'],
      [3, 'three'],
      [4, 'four'],
    ])
  })

  it('should iterate values in sorted key order', () => {
    const map = new SortedMap<number, string>(
      [
        [3, 'three'],
        [1, 'one'],
        [4, 'four'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    const values = Array.from(map.values())
    expect(values).toEqual(['one', 'two', 'three', 'four'])
  })

  it('should work with Symbol.iterator', () => {
    const map = new SortedMap<number, string>(
      [
        [3, 'three'],
        [1, 'one'],
        [4, 'four'],
        [2, 'two'],
      ],
      (a, b) => a - b,
    )
    const result = [...map]
    expect(result).toEqual([
      [1, 'one'],
      [2, 'two'],
      [3, 'three'],
      [4, 'four'],
    ])
  })

  it('should work with custom objects as keys', () => {
    type Person = { name: string; age: number }
    const alice = { name: 'Alice', age: 30 }
    const bob = { name: 'Bob', age: 25 }
    const charlie = { name: 'Charlie', age: 35 }

    const map = new SortedMap<Person, string>(
      [
        [alice, 'developer'],
        [bob, 'designer'],
        [charlie, 'manager'],
      ],
      (a, b) => a.age - b.age,
    )

    const keys = Array.from(map.keys())
    expect(keys[0].name).toBe('Bob')
    expect(keys[1].name).toBe('Alice')
    expect(keys[2].name).toBe('Charlie')

    const values = Array.from(map.values())
    expect(values).toEqual(['designer', 'developer', 'manager'])
  })
})
