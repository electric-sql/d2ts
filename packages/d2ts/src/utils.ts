/**
 * A map that uses WeakRefs to store objects, and automatically removes them when
 * they are no longer referenced.
 */
export class WeakRefMap<K, V extends object> {
  private cacheMap = new Map<K, WeakRef<V>>()
  private finalizer = new FinalizationRegistry((key: K) => {
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

/**
 * A sorted set that maintains a sorted list of values.
 *
 * This implementation maintains a cache of the sorted values, which is invalidated
 * when the set is modified.
 * There will be more performant implementations, particularly if you can drop down
 * to a lower level language, but in javascript this is probably enough for most
 * use cases.
 */
export class SortedSet<T> extends Set<T> {
  private compare?: (a: T, b: T) => number
  private sortedValuesCache?: T[]

  constructor(values?: Iterable<T>, compare?: (a: T, b: T) => number) {
    super(values)
    this.compare = compare
    this.sortedValuesCache = undefined
  }

  private invalidateCache() {
    this.sortedValuesCache = undefined
  }

  private get sortedValues() {
    if (!this.sortedValuesCache) {
      // Use super.values() to avoid calling our overridden values() method
      // which would cause an infinite recursion
      this.sortedValuesCache = Array.from(super.values()).sort(this.compare)
    }
    return this.sortedValuesCache
  }

  add(value: T): this {
    if (!this.has(value)) {
      super.add(value)
      this.invalidateCache()
    }
    return this
  }

  asArray(): T[] {
    return [...this.sortedValues]
  }

  clear(): void {
    super.clear()
    this.invalidateCache()
  }

  delete(value: T): boolean {
    if (super.delete(value)) {
      this.invalidateCache()
      return true
    }
    return false
  }

  entries(): IterableIterator<[T, T]> {
    const sortedValues = this.sortedValues
    function* entries() {
      for (const value of sortedValues!.values()) {
        yield [value, value] as [T, T]
      }
    }
    return entries()
  }

  findIndex(predicate: (value: T) => boolean): number {
    return this.sortedValues.findIndex(predicate)
  }

  findLastIndex(predicate: (value: T) => boolean): number {
    return this.sortedValues.findLastIndex(predicate)
  }

  forEach(
    callbackfn: (value: T, value2: T, set: SortedSet<T>) => void,
    thisArg?: any,
  ): void {
    const sortedValues = this.sortedValues
    if (thisArg) {
      sortedValues!.forEach((value, _) =>
        callbackfn.call(thisArg, value, value, this),
      )
    } else {
      sortedValues!.forEach((value, _) => callbackfn(value, value, this))
    }
  }

  indexOf(value: T): number {
    return this.sortedValues.indexOf(value)
  }

  keys(): IterableIterator<T> {
    return this.values()
  }

  lastIndexOf(value: T): number {
    return this.sortedValues.lastIndexOf(value)
  }

  pop(): T | undefined {
    const value = this.sortedValues.pop()
    if (value) {
      this.delete(value)
    }
    return value
  }

  shift(): T | undefined {
    const value = this.sortedValues.shift()
    if (value) {
      this.delete(value)
    }
    return value
  }

  slice(start: number, end: number): SortedSet<T> {
    return new SortedSet<T>(this.sortedValues.slice(start, end))
  }

  values(): IterableIterator<T> {
    return this.sortedValues.values()
  }

  valueAt(index: number): T {
    return this.sortedValues[index]
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this.values()
  }
}

/**
 * A sorted map that maintains a sorted list of keys.
 *
 * Uses a `SortedSet` to store the keys, and a `Map` to store the values.
 */
export class SortedMap<K, V> extends Map<K, V> {
  private sortedKeys: SortedSet<K>

  constructor(entries?: Iterable<[K, V]>, compare?: (a: K, b: K) => number) {
    super(entries)
    this.sortedKeys = new SortedSet<K>(
      (entries ? [...entries] : []).map(([key]) => key),
      compare,
    )
  }

  asMap(): Map<K, V> {
    return new Map(this)
  }

  clear(): void {
    super.clear()
    this.sortedKeys!.clear()
  }

  delete(key: K): boolean {
    if (super.delete(key)) {
      this.sortedKeys!.delete(key)
      return true
    }
    return false
  }

  entries(): IterableIterator<[K, V]> {
    const sortedKeys = this.sortedKeys
    const map = this
    function* entries() {
      for (const key of sortedKeys.values()) {
        yield [key, map.get(key)!] as [K, V]
      }
    }
    return entries()
  }

  findIndex(predicate: (key: K) => boolean): number {
    return this.sortedKeys.findIndex(predicate)
  }

  findIndexValue(predicate: (value: V) => boolean): number {
    return this.sortedKeys.findIndex((key) => predicate(this.get(key)!))
  }

  findLastIndex(predicate: (key: K) => boolean): number {
    return this.sortedKeys.findLastIndex(predicate)
  }

  findLastIndexValue(predicate: (value: V) => boolean): number {
    return this.sortedKeys.findLastIndex((key) => predicate(this.get(key)!))
  }

  forEach(
    callbackfn: (value: V, key: K, map: SortedMap<K, V>) => void,
    thisArg?: any,
  ): void {
    const sortedKeys = this.sortedKeys
    const map = this
    if (thisArg) {
      sortedKeys.forEach((key, _) =>
        callbackfn.call(thisArg, map.get(key)!, key, map),
      )
    } else {
      sortedKeys.forEach((key, _) => callbackfn(map.get(key)!, key, map))
    }
  }

  indexOf(key: K): number {
    return this.sortedKeys.indexOf(key)
  }

  indexOfValue(value: V): number {
    return this.sortedKeys.findIndex((key) => this.get(key)! === value)
  }

  keys(): IterableIterator<K> {
    return this.sortedKeys.values()
  }

  lastIndexOf(key: K): number {
    return this.sortedKeys.lastIndexOf(key)
  }

  lastIndexOfValue(value: V): number {
    return this.sortedKeys.findLastIndex((key) => this.get(key)! === value)
  }

  pop(): [K, V] {
    const key = this.sortedKeys.pop()
    if (key) {
      const value = this.get(key)!
      this.delete(key)
      return [key, value]
    }
    throw new Error('SortedMap is empty')
  }

  set(key: K, value: V): this {
    super.set(key, value)
    // We need to check if the sortedKeys is already initialized as `add` will be
    // called using the constructor, which will call `super.set` again
    this.sortedKeys?.add(key)
    return this
  }

  shift(): [K, V] {
    const key = this.sortedKeys.shift()
    if (key) {
      const value = this.get(key)!
      this.delete(key)
      return [key, value]
    }
    throw new Error('SortedMap is empty')
  }

  slice(start: number, end: number): SortedMap<K, V> {
    return new SortedMap<K, V>(
      this.sortedKeys
        .slice(start, end)
        .asArray()
        .map((key) => [key, this.get(key)!]),
    )
  }

  values(): IterableIterator<V> {
    const sortedKeys = this.sortedKeys
    const map = this
    function* values() {
      for (const key of sortedKeys.values()) {
        yield map.get(key)!
      }
    }
    return values()
  }

  [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.entries()
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
