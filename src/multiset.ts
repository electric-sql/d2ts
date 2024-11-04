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

  repr(): string {
    return `MultiSet(${JSON.stringify(this.#inner, null, 2)})`
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
    out.push(...this.#inner)
    out.push(...other.#inner)
    return new MultiSet(out)
  }

  /**
   * Produce as output a collection that is logically equivalent to the input
   * but which combines identical instances of the same record into one
   * (record, multiplicity) pair.
   */
  consolidate(): MultiSet<T> {
    const consolidated = new Map<string, number>()

    for (const [data, multiplicity] of this.#inner) {
      const key = JSON.stringify(data)
      const currentCount = consolidated.get(key) || 0
      consolidated.set(key, currentCount + multiplicity)
    }

    const result: MultiSetArray<T> = []
    for (const [key, multiplicity] of consolidated.entries()) {
      if (multiplicity !== 0) {
        result.push([JSON.parse(key) as T, multiplicity])
      }
    }

    result.sort((a, b) =>
      JSON.stringify(a[0]).localeCompare(JSON.stringify(b[0])),
    )
    return new MultiSet(result)
  }

  /**
   * Match pairs (k, v1) and (k, v2) from the two input collections and produce (k, (v1, v2)).
   */
  join<U>(other: MultiSet<KeyedData<U>>): MultiSet<KeyedData<[T, U]>> {
    // TODO: validate that both are keyed collections
    const out: MultiSetArray<KeyedData<[T, U]>> = []

    for (const [[k1, v1], d1] of this.#inner as MultiSetArray<KeyedData<T>>) {
      for (const [[k2, v2], d2] of other.#inner) {
        if (k1 === k2) {
          out.push([[k1, [v1, v2]], d1 * d2])
        }
      }
    }

    return new MultiSet(out)
  }

  /**
   * Apply a reduction function to all record values, grouped by key.
   * This method only works on KeyedData types and returns KeyedData results.
   */
  reduce<U>(f: (vals: [T, number][]) => [U, number][]): MultiSet<KeyedData<U>> {
    // TODO: validate that the collection is keyed
    const keys = new Map<string, [T, number][]>()
    const out: MultiSetArray<KeyedData<U>> = []

    for (const [[key, val], multiplicity] of this.#inner as MultiSetArray<
      KeyedData<T>
    >) {
      const existing = keys.get(key) || []
      existing.push([val, multiplicity])
      keys.set(key, existing)
    }

    for (const [key, vals] of keys.entries()) {
      const results = f(vals)
      for (const [val, multiplicity] of results) {
        out.push([[key, val], multiplicity])
      }
    }

    return new MultiSet(out)
  }

  /**
   * Count the number of times each key occurs in the collection.
   */
  count(): MultiSet<KeyedData<number>> {
    return this.reduce<number>((vals: [T, number][]): [number, number][] => {
      let out = 0
      for (const [_, multiplicity] of vals) {
        out += multiplicity
      }
      return [[out, 1]]
    })
  }

  /**
   * Produce the sum of all the values paired with a key, for all keys in the collection.
   */
  sum(): MultiSet<KeyedData<number>> {
    return this.reduce<number>((vals: [T, number][]): [number, number][] => {
      let out = 0
      for (const [val, multiplicity] of vals) {
        out += (val as unknown as number) * multiplicity
      }
      return [[out, 1]]
    })
  }

  /**
   * Produce the minimum value associated with each key in the collection.
   *
   * Note that no record may have negative multiplicity when computing the min,
   * as it is unclear what exactly the minimum record is in that case.
   */
  min(): MultiSet<KeyedData<T>> {
    return this.reduce<T>((vals: [T, number][]): [T, number][] => {
      const consolidated = new Map<T, [T, number]>()

      for (const [val, multiplicity] of vals) {
        const current = consolidated.get(val)?.[1] || 0
        consolidated.set(val, [val, current + multiplicity])
      }

      const validVals = Array.from(consolidated.values()).filter(
        ([_, multiplicity]) => multiplicity !== 0,
      )

      if (validVals.length === 0) return []

      let minEntry = validVals[0]
      for (const entry of validVals) {
        if (entry[1] <= 0) {
          throw new Error(
            'Negative multiplicities not allowed in min operation',
          )
        }
        if (entry[0] < minEntry[0]) {
          minEntry = entry
        }
      }

      return [[minEntry[0], 1]]
    })
  }

  /**
   * Produce the maximum value associated with each key in the collection.
   *
   * Note that no record may have negative multiplicity when computing the max,
   * as it is unclear what exactly the maximum record is in that case.
   */
  max(): MultiSet<KeyedData<T>> {
    return this.reduce<T>((vals: [T, number][]): [T, number][] => {
      const consolidated = new Map<T, [T, number]>()

      for (const [val, multiplicity] of vals) {
        const current = consolidated.get(val)?.[1] || 0
        consolidated.set(val, [val, current + multiplicity])
      }

      const validVals = Array.from(consolidated.values()).filter(
        ([_, multiplicity]) => multiplicity !== 0,
      )

      if (validVals.length === 0) return []

      let maxEntry = validVals[0]
      for (const entry of validVals) {
        if (entry[1] <= 0) {
          throw new Error(
            'Negative multiplicities not allowed in max operation',
          )
        }
        if (entry[0] > maxEntry[0]) {
          maxEntry = entry
        }
      }

      return [[maxEntry[0], 1]]
    })
  }

  /**
   * Reduce the collection to a set of elements (from a multiset).
   *
   * Note that no record may have negative multiplicity when producing this set,
   * as elements of sets may only have multiplicity one, and it is unclear that is
   * an appropriate output for elements with negative multiplicity.
   */
  distinct(): MultiSet<KeyedData<T>> {
    return this.reduce((vals: [T, number][]): [T, number][] => {
      const consolidated = new Map<string, [T, number]>()

      for (const [val, multiplicity] of vals) {
        const key = JSON.stringify(val)
        const current = consolidated.get(key)?.[1] || 0
        consolidated.set(key, [val, current + multiplicity])
      }

      const validVals = Array.from(consolidated.values()).filter(
        ([_, multiplicity]) => multiplicity !== 0,
      )

      for (const [_, multiplicity] of validVals) {
        if (multiplicity <= 0) {
          throw new Error(
            'Negative multiplicities not allowed in distinct operation',
          )
        }
      }

      return validVals.map(([val, _]) => [val, 1])
    })
  }

  /**
   * Repeatedly invoke a function f on a collection, and return the result
   * of applying the function an infinite number of times (fixedpoint).
   *
   * Note that if the function does not converge to a fixedpoint this implementation
   * will run forever.
   */
  iterate(f: (collection: MultiSet<T>) => MultiSet<T>): MultiSet<T> {
    let curr = new MultiSet(this.#inner)

    while (true) {
      const result = f(curr)
      if (JSON.stringify(result.#inner) === JSON.stringify(curr.#inner)) {
        break
      }
      curr = result
    }

    return curr
  }

  extend(other: MultiSet<T> | MultiSetArray<T>): void {
    if (other instanceof MultiSet) {
      this.#inner.push(...other.getInner())
    } else {
      this.#inner.push(...other)
    }
  }

  getInner(): MultiSetArray<T> {
    return this.#inner
  }
}
