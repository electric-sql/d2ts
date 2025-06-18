import { MultiSet } from './multiset.js'
import { DefaultMap, hash } from './utils.js'

/**
 * A map from a difference collection trace's keys -> (value, multiplicities) that changed.
 * Used in operations like join and reduce where the operation needs to
 * exploit the key-value structure of the data to run efficiently.
 */
export class Index<K, V> {
  #inner: DefaultMap<K, DefaultMap<string, [V, number]>>

  constructor() {
    this.#inner = new DefaultMap<K, DefaultMap<string, [V, number]>>(
      () =>
        new DefaultMap<string, [V, number]>(() => [undefined as any as V, 0]),
    )
    // #inner is as map of:
    // {
    //   [key]: {
    //     [hash(value)]: [value, multiplicity]
    //   }
    // }
  }

  toString(indent = false): string {
    return `Index(${JSON.stringify(
      [...this.#inner].map(([k, valueMap]) => [k, [...valueMap]]),
      undefined,
      indent ? '  ' : undefined,
    )})`
  }

  get(key: K): [V, number][] {
    const valueMap = this.#inner.get(key)
    return [...valueMap.values()]
  }

  getMultiplicity(key: K, value: V): number {
    const valueMap = this.#inner.get(key)
    const valueHash = hash(value)
    const [, multiplicity] = valueMap.get(valueHash)
    return multiplicity
  }

  entries() {
    return this.#inner.entries()
  }

  keys() {
    return this.#inner.keys()
  }

  has(key: K): boolean {
    return this.#inner.has(key)
  }

  get size(): number {
    return this.#inner.size
  }

  addValue(key: K, value: [V, number]): void {
    const [val, multiplicity] = value
    const valueMap = this.#inner.get(key)
    const valueHash = hash(val)
    const [, existingMultiplicity] = valueMap.get(valueHash)
    const newMultiplicity = existingMultiplicity + multiplicity
    if (multiplicity !== 0) {
      if (newMultiplicity === 0) {
        valueMap.delete(valueHash)
      } else {
        valueMap.set(valueHash, [val, newMultiplicity])
      }
    }
  }

  append(other: Index<K, V>): void {
    for (const [key, otherValueMap] of other.entries()) {
      const thisValueMap = this.#inner.get(key)
      for (const [
        valueHash,
        [value, multiplicity],
      ] of otherValueMap.entries()) {
        const [, existingMultiplicity] = thisValueMap.get(valueHash)
        const newMultiplicity = existingMultiplicity + multiplicity
        if (newMultiplicity === 0) {
          thisValueMap.delete(valueHash)
        } else {
          thisValueMap.set(valueHash, [value, newMultiplicity])
        }
      }
    }
  }

  join<V2>(other: Index<K, V2>): MultiSet<[K, [V, V2]]> {
    const result: [[K, [V, V2]], number][] = []

    // We want to iterate over the smaller of the two indexes to reduce the
    // number of operations we need to do.
    if (this.size <= other.size) {
      for (const [key, valueMap] of this.entries()) {
        if (!other.has(key)) continue
        const otherValues = other.get(key)
        for (const [val1, mul1] of valueMap.values()) {
          for (const [val2, mul2] of otherValues) {
            if (mul1 !== 0 && mul2 !== 0) {
              result.push([[key, [val1, val2]], mul1 * mul2])
            }
          }
        }
      }
    } else {
      for (const [key, otherValueMap] of other.entries()) {
        if (!this.has(key)) continue
        const values = this.get(key)
        for (const [val2, mul2] of otherValueMap.values()) {
          for (const [val1, mul1] of values) {
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
