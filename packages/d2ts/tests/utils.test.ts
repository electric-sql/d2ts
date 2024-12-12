import { describe, it, expect } from 'vitest'
import { DefaultMap, WeakRefMap } from '../src/utils.js'

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
