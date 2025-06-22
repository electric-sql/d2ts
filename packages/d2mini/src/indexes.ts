import { MultiSet } from './multiset.js'
import { DefaultMap, hash } from './utils.js'

/**
 * A map from a difference collection trace's keys -> (value, multiplicities) that changed.
 * Used in operations like join and reduce where the operation needs to
 * exploit the key-value structure of the data to run efficiently.
 */
export class Index<K, V> {
  #inner: DefaultMap<K, [V, number][]>
  #changedKeys: Set<K>

  constructor() {
    this.#inner = new DefaultMap<K, [V, number][]>(() => [])
    this.#changedKeys = new Set<K>()
  }

  toString(indent = false): string {
    return `Index(${JSON.stringify(
      [...this.#inner],
      undefined,
      indent ? '  ' : undefined,
    )})`
  }

  get(key: K): [V, number][] {
    return this.#inner.get(key)
  }

  entries() {
    return this.#inner.entries()
  }

  keys() {
    return this.#inner.keys()
  }

  has(key: K): boolean {
    return this.#inner.has(key) && this.#inner.get(key).length > 0
  }

  get size(): number {
    let count = 0
    for (const [, values] of this.#inner.entries()) {
      if (values.length > 0) {
        count++
      }
    }
    return count
  }

  addValue(key: K, value: [V, number]): void {
    const values = this.#inner.get(key)
    values.push(value)
    this.#changedKeys.add(key)
  }

  append(other: Index<K, V>): void {
    for (const [key, otherValues] of other.entries()) {
      const thisValues = this.#inner.get(key)
      for (const value of otherValues) {
        thisValues.push(value)
      }
      this.#changedKeys.add(key)
    }
  }

  compact(keys: K[] = []): void {
    // If no keys specified, use the changed keys
    const keysToProcess = keys.length === 0 ? [...this.#changedKeys] : keys

    for (const key of keysToProcess) {
      if (!this.#inner.has(key)) continue

      const values = this.#inner.get(key)
      const consolidated = this.consolidateValues(values)

      // Remove the key entirely and re-add only if there are non-zero values
      this.#inner.delete(key)
      if (consolidated.length > 0) {
        this.#inner.get(key).push(...consolidated)
      }
    }

    // Clear the changed keys after compaction
    if (keys.length === 0) {
      this.#changedKeys.clear()
    } else {
      // Only remove the keys that were explicitly compacted
      for (const key of keys) {
        this.#changedKeys.delete(key)
      }
    }
  }

  private consolidateValues(values: [V, number][]): [V, number][] {
    const consolidated = new Map<string, { value: V; multiplicity: number }>()

    for (const [value, multiplicity] of values) {
      const valueHash = hash(value)
      if (consolidated.has(valueHash)) {
        consolidated.get(valueHash)!.multiplicity += multiplicity
      } else {
        consolidated.set(valueHash, { value, multiplicity })
      }
    }

    return [...consolidated.values()]
      .filter(({ multiplicity }) => multiplicity !== 0)
      .map(({ value, multiplicity }) => [value, multiplicity])
  }

  join<V2>(other: Index<K, V2>): MultiSet<[K, [V, V2]]> {
    const result: [[K, [V, V2]], number][] = []

    // We want to iterate over the smaller of the two indexes to reduce the
    // number of operations we need to do.
    if (this.size <= other.size) {
      for (const [key, values1] of this.entries()) {
        if (!other.has(key)) continue
        const values2 = other.get(key)
        for (const [val1, mul1] of values1) {
          for (const [val2, mul2] of values2) {
            if (mul1 !== 0 && mul2 !== 0) {
              result.push([[key, [val1, val2]], mul1 * mul2])
            }
          }
        }
      }
    } else {
      for (const [key, values2] of other.entries()) {
        if (!this.has(key)) continue
        const values1 = this.get(key)
        for (const [val2, mul2] of values2) {
          for (const [val1, mul1] of values1) {
            if (mul1 !== 0 && mul2 !== 0) {
              result.push([[key, [val1, val2]], mul1 * mul2])
            }
          }
        }
      }
    }

    return new MultiSet(result)
  }
}
