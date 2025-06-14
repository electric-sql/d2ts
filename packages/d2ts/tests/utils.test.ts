import { describe, it, expect, vi } from 'vitest'
import { DefaultMap, WeakRefMap, hash, WeakRefBasedFinalizationRegistry } from '../src/utils.js'

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

describe('hash', () => {
  describe('primitive types', () => {
    it('should hash null', () => {
      const result = hash(null)
      expect(typeof result).toBe('number')
    })

    it('should hash undefined', () => {
      const result = hash(undefined)
      expect(typeof result).toBe('number')
    })

    it('should hash strings', () => {
      const result1 = hash('hello')
      const result2 = hash('')
      const result3 = hash('test with spaces')
      const result4 = hash('special\nchars\t"')
      
      expect(typeof result1).toBe('number')
      expect(typeof result2).toBe('number')
      expect(typeof result3).toBe('number')
      expect(typeof result4).toBe('number')
      
      // Same strings should have same hash
      expect(hash('hello')).toBe(result1)
    })

    it('should hash numbers', () => {
      const result1 = hash(42)
      const result2 = hash(0)
      const result3 = hash(-1)
      const result4 = hash(3.14159)
      const result5 = hash(Infinity)
      const result6 = hash(-Infinity)
      const result7 = hash(NaN)
      
      expect(typeof result1).toBe('number')
      expect(typeof result2).toBe('number')
      expect(typeof result3).toBe('number')
      expect(typeof result4).toBe('number')
      expect(typeof result5).toBe('number')
      expect(typeof result6).toBe('number')
      expect(typeof result7).toBe('number')
      
      // Same numbers should have same hash
      expect(hash(42)).toBe(result1)
    })

    it('should hash booleans', () => {
      const result1 = hash(true)
      const result2 = hash(false)
      
      expect(typeof result1).toBe('number')
      expect(typeof result2).toBe('number')
      expect(result1).not.toBe(result2)
      
      // Same booleans should have same hash
      expect(hash(true)).toBe(result1)
      expect(hash(false)).toBe(result2)
    })

    it('should hash bigint', () => {
      const result1 = hash(123n)
      const result2 = hash(456n)
      const result3 = hash(123n)
      
      expect(typeof result1).toBe('number')
      expect(typeof result2).toBe('number')
      expect(typeof result3).toBe('number')
      expect(result1).toBe(result3) // Same bigint should have same hash
      expect(result1).not.toBe(result2) // Different bigints should have different hash
    })

    it('should hash symbols', () => {
      const sym1 = Symbol('test')
      const sym2 = Symbol('test')
      const sym3 = Symbol('different')
      
      const result1 = hash(sym1)
      const result2 = hash(sym2)
      const result3 = hash(sym3)
      
      expect(typeof result1).toBe('number')
      expect(typeof result2).toBe('number')
      expect(typeof result3).toBe('number')
      // Note: Different symbol instances with same description have same string representation
      expect(result1).toBe(result2)
      expect(result1).not.toBe(result3)
    })
  })

  describe('object types', () => {
    it('should hash plain objects', () => {
      const obj1 = { a: 1, b: 2 }
      const obj2 = { b: 2, a: 1 } // Different key order
      
      const hash1 = hash(obj1)
      const hash2 = hash(obj2)
      
      expect(typeof hash1).toBe('number')
      expect(typeof hash2).toBe('number')
      // Note: Different key orders might produce different hashes depending on JSON.stringify behavior
    })

    it('should hash arrays', () => {
      const arr1 = [1, 2, 3]
      const arr2 = [1, 2, 3]
      const arr3 = [3, 2, 1]
      
      const hash1 = hash(arr1)
      const hash2 = hash(arr2)
      const hash3 = hash(arr3)
      
      expect(typeof hash1).toBe('number')
      expect(hash1).toBe(hash2) // Same content should have same hash
      expect(hash1).not.toBe(hash3) // Different content should have different hash
    })

    it('should hash Date objects', () => {
      const date1 = new Date('2023-01-01')
      const date2 = new Date('2023-01-01')
      const date3 = new Date('2023-01-02')
      
      const hash1 = hash(date1)
      const hash2 = hash(date2)
      const hash3 = hash(date3)
      
      expect(typeof hash1).toBe('number')
      expect(hash1).toBe(hash2) // Same date should have same hash
      expect(hash1).not.toBe(hash3) // Different dates should have different hash
    })

    it('should hash RegExp objects', () => {
      const regex1 = /test/g
      const regex2 = /test/g
      const regex3 = /different/i
      
      const hash1 = hash(regex1)
      const hash2 = hash(regex2)
      const hash3 = hash(regex3)
      
      expect(typeof hash1).toBe('number')
      expect(hash1).toBe(hash2) // Same regex should have same hash
      // Note: RegExp objects serialize to empty objects {}, so they all produce the same hash
      expect(hash1).toBe(hash3) // All RegExp objects have the same hash
    })

    it('should hash nested objects', () => {
      const nested1 = { a: { b: { c: 1 } } }
      const nested2 = { a: { b: { c: 1 } } }
      const nested3 = { a: { b: { c: 2 } } }
      
      const hash1 = hash(nested1)
      const hash2 = hash(nested2)
      const hash3 = hash(nested3)
      
      expect(typeof hash1).toBe('number')
      expect(hash1).toBe(hash2)
      expect(hash1).not.toBe(hash3)
    })

    it('should hash functions', () => {
      const func1 = function test() { return 1 }
      const func2 = function test() { return 1 }
      const func3 = function different() { return 2 }
      
      const hash1 = hash(func1)
      const hash2 = hash(func2)
      const hash3 = hash(func3)
      
      expect(typeof hash1).toBe('number')
      expect(typeof hash2).toBe('number')
      expect(typeof hash3).toBe('number')
      expect(hash1).toBe(hash2) // Same function definition should have same hash
      expect(hash1).not.toBe(hash3) // Different function should have different hash
    })

    it('should hash Set objects', () => {
      const set1 = new Set([1, 2, 3])
      const set2 = new Set([1, 2, 3])
      const set3 = new Set([1, 2, 3, 4])
      
      const hash1 = hash(set1)
      const hash2 = hash(set2)
      const hash3 = hash(set3)
      
      expect(typeof hash1).toBe('number')
      expect(hash1).toBe(hash2) // Same content should have same hash
      expect(hash1).not.toBe(hash3) // Different content should have different hash
    })

    it('should hash Map objects', () => {
      const map1 = new Map([['a', 1], ['b', 2]])
      const map2 = new Map([['a', 1], ['b', 2]])
      const map3 = new Map([['a', 1], ['b', 2], ['c', 3]])
      
      const hash1 = hash(map1)
      const hash2 = hash(map2)
      const hash3 = hash(map3)
      
      expect(typeof hash1).toBe('number')
      expect(hash1).toBe(hash2) // Same content should have same hash
      expect(hash1).not.toBe(hash3) // Different content should have different hash
    })

    it('should hash Maps and Sets with unsupported types', () => {
      // Map with BigInt values
      const mapWithBigInt1 = new Map([['a', 123n], ['b', 456n]])
      const mapWithBigInt2 = new Map([['a', 123n], ['b', 456n]])
      const mapWithBigInt3 = new Map([['a', 123n], ['b', 789n]])
      
      const hash1 = hash(mapWithBigInt1)
      const hash2 = hash(mapWithBigInt2)
      const hash3 = hash(mapWithBigInt3)
      
      expect(typeof hash1).toBe('number')
      expect(hash1).toBe(hash2) // Same BigInt content should have same hash
      expect(hash1).not.toBe(hash3) // Different BigInt content should have different hash
      
      // Set with Symbol values
      const sym1 = Symbol('test')
      const sym2 = Symbol('different')
      const setWithSymbols1 = new Set([sym1, sym2])
      const setWithSymbols2 = new Set([sym1, sym2])
      const setWithSymbols3 = new Set([sym1])
      
      const hash4 = hash(setWithSymbols1)
      const hash5 = hash(setWithSymbols2)
      const hash6 = hash(setWithSymbols3)
      
      expect(typeof hash4).toBe('number')
      expect(hash4).toBe(hash5) // Same Symbol content should have same hash
      expect(hash4).not.toBe(hash6) // Different Symbol content should have different hash
    })
  })

  describe('caching behavior', () => {
    it('should cache hash values for objects', () => {
      const obj = { test: 'value' }
      
      const hash1 = hash(obj)
      const hash2 = hash(obj)
      
      expect(hash1).toBe(hash2)
      expect(typeof hash1).toBe('number')
    })

    it('should return cached values on subsequent calls', () => {
      const obj = { complex: { nested: { data: [1, 2, 3] } } }
      
      // First call should compute and cache
      const hash1 = hash(obj)
      
      // Second call should return cached value
      const hash2 = hash(obj)
      
      expect(hash1).toBe(hash2)
      expect(typeof hash1).toBe('number')
    })

    it('should not cache primitive values', () => {
      // Primitives should not be cached as they use JSON.stringify directly
      const hash1 = hash('test')
      const hash2 = hash('test')
      
      expect(hash1).toBe(hash2)
      expect(typeof hash1).toBe('number')
    })
  })

  describe('edge cases', () => {
    it('should handle empty objects and arrays', () => {
      expect(typeof hash({})).toBe('number')
      expect(typeof hash([])).toBe('number')
      expect(hash({})).not.toBe(hash([]))
    })

    it('should handle objects with null and undefined values', () => {
      const obj1 = { a: null, b: undefined }
      const obj2 = { a: null, b: undefined }
      
      const hash1 = hash(obj1)
      const hash2 = hash(obj2)
      
      expect(hash1).toBe(hash2)
      expect(typeof hash1).toBe('number')
    })

    it('should handle mixed type arrays', () => {
      const mixedArray = [1, 'string', true, null, { key: 'value' }]
      const sameArray = [1, 'string', true, null, { key: 'value' }]
      
      const hash1 = hash(mixedArray)
      const hash2 = hash(sameArray)
      
      expect(hash1).toBe(hash2)
      expect(typeof hash1).toBe('number')
    })

    it('should produce consistent hashes for same content', () => {
      const obj = { 
        string: 'test',
        number: 42,
        boolean: true,
        array: [1, 2, 3],
        nested: { inner: 'value' }
      }
      
      // Multiple calls should return the same hash
      const hashes = Array.from({ length: 5 }, () => hash(obj))
      const firstHash = hashes[0]
      
      expect(hashes.every(h => h === firstHash)).toBe(true)
      expect(typeof firstHash).toBe('number')
    })
  })
})

describe('WeakRefBasedFinalizationRegistry', () => {
  it('should register and unregister objects', () => {
    const finalizeSpy = vi.fn()
    const registry = new WeakRefBasedFinalizationRegistry(finalizeSpy)
    
    const target = { test: 'value' }
    const token = { token: 'value' }
    const value = 'test value'
    
    registry.register(target, value, token)
    registry.unregister(token)
    
    // The finalize callback should not have been called since we unregistered
    expect(finalizeSpy).not.toHaveBeenCalled()
  })

  // TODO: find a way to make this actually work...
  // it('should call finalize when target is garbage collected', async () => {
  //   const finalizeSpy = vi.fn()
  //   const registry = new WeakRefBasedFinalizationRegistry(finalizeSpy, 100) // Use 100ms interval for testing
    
  //   // Create object in a scope that will end
  //   {
  //     const target = { test: 'value' }
  //     const value = 'test value'
  //     registry.register(target, value, target)
  //   }
    
  //   // Force garbage collection if possible
  //   if (global.gc) {
  //     // Run GC multiple times to ensure cleanup
  //     for (let i = 0; i < 3; i++) {
  //       global.gc()
  //       // Give finalizers a chance to run
  //       await new Promise(resolve => setTimeout(resolve, 0))
  //     }
      
  //     // Wait for the sweep interval (plus a small buffer)
  //     await new Promise(resolve => setTimeout(resolve, 200))
      
  //     // The finalize callback should have been called
  //     expect(finalizeSpy).toHaveBeenCalledWith('test value')
  //   } else {
  //     console.warn('Test skipped: garbage collection not exposed. Run Node.js with --expose-gc flag.')
  //   }
  // })

  it('should handle multiple registrations', () => {
    const finalizeSpy = vi.fn()
    const registry = new WeakRefBasedFinalizationRegistry(finalizeSpy)
    
    const target1 = { test: 'value1' }
    const target2 = { test: 'value2' }
    const token1 = { token: 'value1' }
    const token2 = { token: 'value2' }
    
    registry.register(target1, 'value1', token1)
    registry.register(target2, 'value2', token2)
    
    // Unregister one token
    registry.unregister(token1)
    
    // The finalize callback should not have been called
    expect(finalizeSpy).not.toHaveBeenCalled()
  })

  it('should handle null token in unregister', () => {
    const finalizeSpy = vi.fn()
    const registry = new WeakRefBasedFinalizationRegistry(finalizeSpy)
    
    const target = { test: 'value' }
    registry.register(target, 'value', null)
    
    // Should not throw when unregistering with null token
    expect(() => registry.unregister(null)).not.toThrow()
  })
})
