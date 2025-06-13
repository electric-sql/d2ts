import murmurhash from 'murmurhash-js'

/**
 * A map that uses WeakRefs to store objects, and automatically removes them when
 * they are no longer referenced.
 */
export class WeakRefMap<K, V extends object> {
  private cacheMap = new Map<K, WeakRef<V>>()
  private finalizer = new AnyFinalizationRegistry((key: K) => {
    this.cacheMap.delete(key)
  })

  set(key: K, value: V): void {
    const cache = this.get(key)
    if (cache) {
      if (cache === value) return
      this.finalizer.unregister(cache)
    }
    this.cacheMap.set(key, new WeakRef(value))
    this.finalizer.register(value, key, value)
  }

  get(key: K): V | null {
    return this.cacheMap.get(key)?.deref() ?? null
  }
}

/**
 * A map that returns a default value for keys that are not present.
 */
export class DefaultMap<K, V> extends Map<K, V> {
  constructor(
    private defaultValue: () => V,
    entries?: Iterable<[K, V]>,
  ) {
    super(entries)
  }

  get(key: K): V {
    if (!this.has(key)) {
      this.set(key, this.defaultValue())
    }
    return super.get(key)!
  }

  /**
   * Update the value for a key using a function.
   */
  update(key: K, updater: (value: V) => V): V {
    const value = this.get(key)
    const newValue = updater(value)
    this.set(key, newValue)
    return newValue
  }
}

// JS engines have various limits on how many args can be passed to a function
// with a spread operator, so we need to split the operation into chunks
// 32767 is the max for Chrome 14, all others are higher
// TODO: investigate the performance of this and other approaches
const chunkSize = 30000
export function chunkedArrayPush(array: unknown[], other: unknown[]) {
  if (other.length <= chunkSize) {
    array.push(...other)
  } else {
    for (let i = 0; i < other.length; i += chunkSize) {
      const chunk = other.slice(i, i + chunkSize)
      array.push(...chunk)
    }
  }
}

const hashCache = new WeakMap()

/**
 * Replacer function for JSON.stringify that converts unsupported types to strings
 */
function hashReplacer(_key: string, value: any): any {
  if (typeof value === 'bigint') {
    return String(value)
  } else if (typeof value === 'symbol') {
    return String(value)
  } else if (typeof value === 'function') {
    return String(value)
  } else if (value === undefined) {
    return 'undefined'
  } else if (value instanceof Map) {
    return `Map(${JSON.stringify(Array.from(value.entries()), hashReplacer)})`
  } else if (value instanceof Set) {
    return `Set(${JSON.stringify(Array.from(value.values()), hashReplacer)})`
  }
  return value
}

/**
 * A hash method that caches the hash of a value in a week map
 */
export function hash(data: any): string | number {
  if (
    data === null ||
    data === undefined ||
    (typeof data !== 'object' && typeof data !== 'function')
  ) {
    // Can't be cached in the weak map because it's not an object
    const serialized = JSON.stringify(data, hashReplacer)
    return murmurhash.murmur3(serialized)
  }

  if (hashCache.has(data)) {
    return hashCache.get(data)
  }

  const serialized = JSON.stringify(data, hashReplacer)
  const hashValue = murmurhash.murmur3(JSON.stringify(serialized))
  hashCache.set(data, hashValue)
  return hashValue
}

/**
 * This is a mock implementation of FinalizationRegistry which uses WeakRef to
 * track the target objects. It's used in environments where FinalizationRegistry
 * is not available but WeakRef is (e.g. React Native >=0.75 on New Architecture).
 * Based on https://gist.github.com/cray0000/abecb1ca71fd28a1d8efff2be9e0f6c5
 * MIT License - Copyright Cray0000
 */
export class WeakRefBasedFinalizationRegistry {
  private counter = 0
  private registrations = new Map()
  private sweepTimeout: NodeJS.Timeout | undefined
  private finalize: (value: any) => void
  private sweepIntervalMs = 10_000

  constructor(finalize: (value: any) => void, sweepIntervalMs?: number) {
    this.finalize = finalize
    if (sweepIntervalMs !== undefined) {
      this.sweepIntervalMs = sweepIntervalMs
    }
  }

  register(target: any, value: any, token: any) {
    this.registrations.set(this.counter, {
      targetRef: new WeakRef(target),
      tokenRef: token != null ? new WeakRef(token) : undefined,
      value,
    })
    this.counter++
    this.scheduleSweep()
  }

  unregister(token: any) {
    if (token == null) return
    this.registrations.forEach((registration, key) => {
      if (registration.tokenRef?.deref() === token) {
        this.registrations.delete(key)
      }
    })
  }

  // Bound so it can be used directly as setTimeout callback.
  private sweep = () => {
    clearTimeout(this.sweepTimeout)
    this.sweepTimeout = undefined

    this.registrations.forEach((registration, key) => {
      if (registration.targetRef.deref() !== undefined) return
      const value = registration.value
      this.registrations.delete(key)
      this.finalize(value)
    })

    if (this.registrations.size > 0) this.scheduleSweep()
  }

  private scheduleSweep() {
    if (this.sweepTimeout) return
    this.sweepTimeout = setTimeout(this.sweep, this.sweepIntervalMs)
  }
}

const AnyFinalizationRegistry =
  typeof FinalizationRegistry !== 'undefined'
    ? FinalizationRegistry
    : WeakRefBasedFinalizationRegistry
