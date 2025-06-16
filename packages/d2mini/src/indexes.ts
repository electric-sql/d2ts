import { MultiSet } from './multiset.js'
import { DefaultMap, hash } from './utils.js'

export interface IndexType<K, V> {
  reconstruct(key: K): [V, number][]
  addValue(key: K, value: [V, number]): void
  append(other: IndexType<K, V>): void
  join<V2>(other: IndexType<K, V2>): MultiSet<[K, [V, V2]]>
  keys(): K[]
  has(key: K): boolean
}

/**
 * A map from a difference collection trace's keys -> (value, multiplicities) that changed.
 * Used in operations like join and reduce where the operation needs to
 * exploit the key-value structure of the data to run efficiently.
 */
export class Index<K, V> implements IndexType<K, V> {
  #inner: DefaultMap<K, [V, number][]>
  #modifiedKeys: Set<K>

  constructor() {
    this.#inner = new DefaultMap<K, [V, number][]>(() => [])
    // #inner is as map of:
    // {
    //   [key]: [[value, multiplicity], ...]
    // }
    this.#modifiedKeys = new Set()
  }

  toString(indent = false): string {
    return `Index(${JSON.stringify(
      [...this.#inner].map(([k, v]) => [k, v]),
      undefined,
      indent ? '  ' : undefined,
    )})`
  }

  reconstruct(key: K): [V, number][] {
    const values = this.#inner.get(key)
    return values.filter(([_, multiplicity]) => multiplicity !== 0)
  }

  get(key: K): [V, number][] {
    return this.#inner.get(key)
  }

  entries(): [K, [V, number][]][] {
    return this.keys().map((key) => [key, this.get(key)])
  }

  addValue(key: K, value: [V, number]): void {
    const [val, multiplicity] = value
    const values = this.#inner.get(key)
    const existingIndex = values.findIndex(([v, _]) => hash(v) === hash(val))
    if (existingIndex >= 0) {
      const [_, existingMultiplicity] = values[existingIndex]
      values[existingIndex] = [val, existingMultiplicity + multiplicity]
    } else {
      values.push([val, multiplicity])
    }
    this.#modifiedKeys.add(key)
  }

  append(other: Index<K, V>): void {
    for (const [key, values] of other.entries()) {
      const thisValues = this.#inner.get(key)
      for (const [value, multiplicity] of values) {
        const existingIndex = thisValues.findIndex(
          ([v, _]) => hash(v) === hash(value),
        )
        if (existingIndex >= 0) {
          const [_, existingMultiplicity] = thisValues[existingIndex]
          thisValues[existingIndex] = [
            value,
            existingMultiplicity + multiplicity,
          ]
        } else {
          thisValues.push([value, multiplicity])
        }
      }
      this.#modifiedKeys.add(key)
    }
  }

  join<V2>(other: Index<K, V2>): MultiSet<[K, [V, V2]]> {
    const result: [K, [V, V2], number][] = []

    // We want to iterate over the smaller of the two indexes to reduce the
    // number of operations we need to do.
    if (this.#inner.size <= other.#inner.size) {
      for (const [key, values] of this.#inner) {
        if (!other.has(key)) continue
        const otherValues = other.get(key)
        for (const [val1, mul1] of values) {
          for (const [val2, mul2] of otherValues) {
            if (mul1 !== 0 && mul2 !== 0) {
              result.push([key, [val1, val2], mul1 * mul2])
            }
          }
        }
      }
    } else {
      for (const [key, otherValues] of other.entries()) {
        if (!this.has(key)) continue
        const values = this.get(key)
        for (const [val2, mul2] of otherValues) {
          for (const [val1, mul1] of values) {
            if (mul1 !== 0 && mul2 !== 0) {
              result.push([key, [val1, val2], mul1 * mul2])
            }
          }
        }
      }
    }

    return new MultiSet(result.map(([k, v, m]) => [[k, v], m]))
  }

  keys(): K[] {
    return Array.from(this.#inner.keys())
  }

  has(key: K): boolean {
    return this.#inner.has(key)
  }
}
