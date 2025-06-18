import murmurhash from 'murmurhash-js'

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
export function hash(data: any): string {
  if (
    data === null ||
    data === undefined ||
    (typeof data !== 'object' && typeof data !== 'function')
  ) {
    // Can't be cached in the weak map because it's not an object
    const serialized = JSON.stringify(data, hashReplacer)
    return murmurhash.murmur3(serialized).toString(16)
  }

  if (hashCache.has(data)) {
    return hashCache.get(data)
  }

  const serialized = JSON.stringify(data, hashReplacer)
  const hashValue = murmurhash.murmur3(JSON.stringify(serialized)).toString(16)
  hashCache.set(data, hashValue)
  return hashValue
}

export function binarySearch<T>(
  array: T[],
  value: T,
  comparator: (a: T, b: T) => number,
): number {
  let low = 0
  let high = array.length
  while (low < high) {
    const mid = Math.floor((low + high) / 2)
    const comparison = comparator(array[mid], value)
    if (comparison < 0) {
      low = mid + 1
    } else if (comparison > 0) {
      high = mid
    } else {
      return mid
    }
  }
  return low
}