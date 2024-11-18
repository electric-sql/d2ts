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
