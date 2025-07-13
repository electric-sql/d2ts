import { chunkedArrayPush } from './utils.js'

export type MultiSetArray<T> = [T, number][]
export type KeyedData<T> = [key: string, value: T]

/**
 * A multiset of data.
 */
export class MultiSet<T> {
  #inner: MultiSetArray<T>

  constructor(data: MultiSetArray<T> = []) {
    this.#inner = data
  }

  toString(indent = false): string {
    return `MultiSet(${JSON.stringify(this.#inner, null, indent ? 2 : undefined)})`
  }

  toJSON(): string {
    return JSON.stringify(Array.from(this.getInner()))
  }

  static fromJSON<T>(json: string): MultiSet<T> {
    return new MultiSet(JSON.parse(json))
  }

  /**
   * Apply a function to all records in the collection.
   */
  map<U>(f: (data: T) => U): MultiSet<U> {
    return new MultiSet(
      this.#inner.map(([data, multiplicity]) => [f(data), multiplicity]),
    )
  }

  /**
   * Filter out records for which a function f(record) evaluates to False.
   */
  filter(f: (data: T) => boolean): MultiSet<T> {
    return new MultiSet(this.#inner.filter(([data, _]) => f(data)))
  }

  /**
   * Negate all multiplicities in the collection.
   */
  negate(): MultiSet<T> {
    return new MultiSet(
      this.#inner.map(([data, multiplicity]) => [data, -multiplicity]),
    )
  }

  /**
   * Concatenate two collections together.
   */
  concat(other: MultiSet<T>): MultiSet<T> {
    const out: MultiSetArray<T> = []
    chunkedArrayPush(out, this.#inner)
    chunkedArrayPush(out, other.getInner())
    return new MultiSet(out)
  }

  /**
   * Produce as output a collection that is logically equivalent to the input
   * but which combines identical instances of the same record into one
   * (record, multiplicity) pair.
   */
  consolidate(): MultiSet<T> {
    const consolidated = new Map<string, { data: T, multiplicity: number }>()

    for (const [data, multiplicity] of this.#inner) {
      const key = JSON.stringify(data)
      const existing = consolidated.get(key)
      const newMultiplicity = (existing?.multiplicity ?? 0) + multiplicity
      
      if (newMultiplicity === 0) {
        consolidated.delete(key)
      } else {
        consolidated.set(key, { data, multiplicity: newMultiplicity })
      }
    }

    return new MultiSet([...consolidated.values()].map(entry => [entry.data, entry.multiplicity]))
  }

  extend(other: MultiSet<T> | MultiSetArray<T>): void {
    const otherArray = other instanceof MultiSet ? other.getInner() : other
    chunkedArrayPush(this.#inner, otherArray)
  }

  getInner(): MultiSetArray<T> {
    return this.#inner
  }
}
