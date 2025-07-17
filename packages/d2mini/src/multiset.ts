export type MultiSetArray<T> = [T, number][]
export type MultiSetMap<T> = Map<T, number>

/**
 * A hybrid multiset that handles both regular values and specific array structures.
 * For primitives it compares by value.
 * For objects, it compares by reference.
 * It also supports arrays formatted as `[v1, v2]` and `[v1, [v2, v3]]` and compares them by value,
 * note that the values inside the array are compared by reference. So two arrays `[v1, v2]` and `[v3, v4]` are equal if `v1 === v3` and `v2 === v4`.
 */
export class MultiSet<T> {
  #regularMap: Map<T, number> = new Map()
  #pairMap: Map<any, Map<any, number>> = new Map() // for [key, value]
  #tripleMap: Map<any, Map<any, Map<any, number>>> = new Map() // for [key, [value1, value2]] format outputted by join

  constructor(data: MultiSetArray<T> = []) {
    data.forEach(([data, multiplicity]) => {
      this.add(data, multiplicity)
    })
  }

  toString(indent = false): string {
    const regular = [...this.#regularMap.entries()]
    const pairs = this.#flattenPairMap()
    const triples = this.#flattenTripleMap()
    const all = [...regular, ...pairs, ...triples]
    return `MultiSet(${JSON.stringify(all, null, indent ? 2 : undefined)})`
  }

  add(data: T, multiplicity: number): void {
    const currentMultiplicity = this.get(data)
    this.set(data, currentMultiplicity + multiplicity)
  }

  set(data: T, multiplicity: number): void {
    if (Array.isArray(data)) {
      this.#setArray(data, multiplicity)
    } else {
      if (multiplicity !== 0) {
        this.#regularMap.set(data, multiplicity)
      } else {
        this.#regularMap.delete(data)
      }
    }
  }

  get(data: T): number {
    if (Array.isArray(data)) {
      return this.#getArray(data)
    } else {
      return this.#regularMap.get(data) ?? 0
    }
  }

  map<U>(f: (data: T) => U): MultiSet<U> {
    const m = new MultiSet<U>()
    for (const [data, multiplicity] of this.iterator()) {
      const newData = f(data)
      const oldMultiplicity = m.get(newData)
      const newMultiplicity = oldMultiplicity + multiplicity
      m.set(newData, newMultiplicity)
    }
    return m
  }

  filter(f: (data: T) => boolean): MultiSet<T> {
    const m = new MultiSet<T>()
    for (const [data, multiplicity] of this.iterator()) {
      if (f(data)) {
        m.set(data, multiplicity)
      }
    }
    return m
  }

  negate(): MultiSet<T> {
    const m = new MultiSet<T>()
    for (const [data, multiplicity] of this.iterator()) {
      m.set(data, -multiplicity)
    }
    return m
  }

  concat(other: MultiSet<T>): MultiSet<T> {
    const m = new MultiSet<T>()
    for (const [data, multiplicity] of this.iterator()) {
      m.set(data, multiplicity)
    }
    for (const [data, multiplicity] of other.iterator()) {
      const oldMultiplicity = m.get(data)
      const newMultiplicity = oldMultiplicity + multiplicity
      m.set(data, newMultiplicity)
    }
    return m
  }

  extend(other: MultiSet<T> | MultiSetArray<T>): void {
    const it = other instanceof MultiSet ? other.iterator() : other
    for (const [data, multiplicity] of it) {
      this.add(data, multiplicity)
    }
  }

  consolidate(): MultiSet<T> {
    // The set is already consolidated
    // just return a copy
    return new MultiSet<T>(this.getInner())
  }

  getInner(): MultiSetArray<T> {
    const regular = [...this.#regularMap.entries()]
    const pairs = this.#flattenPairMap()
    const triples = this.#flattenTripleMap()
    return [...regular, ...pairs, ...triples]
  }

  iterator(): IterableIterator<[T, number]> {
    const allEntries = this.getInner()
    return allEntries[Symbol.iterator]() as IterableIterator<[T, number]>
  }

  /**
   * Sets the multiplicity for array data.
   * Handles [v1, v2] and [v1, [v2, v3]] structures.
   */
  #setArray(data: any[], multiplicity: number): void {
    if (data.length === 2) {
      const [v1, v2] = data
      if (Array.isArray(v2)) {
        // Handle [v1, [v2, v3]] structure
        if (v2.length === 2) {
          const [v2_val, v3] = v2
          if (multiplicity === 0) {
            this.#deleteTriple([v1, v2_val, v3])
          } else {
            const { map } = this.#getTriple([v1, v2_val, v3], true)
            map.set(v3, multiplicity)
          }
        } else {
          throw new Error(
            `MultiSet can't handle arrays of this format. Array should be formatted as [v1, v2] or [v1, [v2, v3]].`,
          )
        }
      } else {
        // Handle [v1, v2] structure
        if (multiplicity === 0) {
          this.#deletePair([v1, v2])
        } else {
          const { map } = this.#getPair([v1, v2], true)
          map.set(v2, multiplicity)
        }
      }
    } else {
      throw new Error(
        `MultiSet can't handle arrays of this format. Array should be formatted as [v1, v2] or [v1, [v2, v3]].`,
      )
    }
  }

  /**
   * Gets the multiplicity for array data.
   */
  #getArray(data: any[]): number {
    if (data.length === 2) {
      const [v1, v2] = data
      if (Array.isArray(v2)) {
        // Handle [v1, [v2, v3]] structure
        if (v2.length === 2) {
          const [v2_val, v3] = v2
          const res = this.#getTriple([v1, v2_val, v3], false)
          if (!res) return 0
          return res.multiplicity ?? 0
        } else {
          throw new Error(
            `MultiSet can't handle arrays of this format. Array should be formatted as [v1, v2] or [v1, [v2, v3]].`,
          )
        }
      } else {
        // Handle [v1, v2] structure
        const res = this.#getPair([v1, v2], false)
        if (!res) return 0
        return res.multiplicity ?? 0
      }
    } else {
      throw new Error(
        `MultiSet can't handle arrays of this format. Array should be formatted as [v1, v2] or [v1, [v2, v3]].`,
      )
    }
  }

  /**
   * Gets or creates the nested map structure for pairs [v1, v2].
   */
  #getPair(
    pair: [any, any],
    create: true,
  ): {
    multiplicity: number | undefined
    map: Map<any, number>
  }
  #getPair(
    pair: [any, any],
    create: false,
  ):
    | {
        multiplicity: number | undefined
        map: Map<any, number>
      }
    | undefined
  #getPair(
    pair: [any, any],
    create = false,
  ):
    | {
        multiplicity: number | undefined
        map: Map<any, number>
      }
    | undefined {
    const [v1, v2] = pair
    let map = this.#pairMap.get(v1)

    if (!map) {
      if (!create) return undefined
      map = new Map()
      this.#pairMap.set(v1, map)
    }

    return {
      multiplicity: map.get(v2),
      map,
    }
  }

  /**
   * Gets or creates the nested map structure for triples [v1, v2, v3].
   */
  #getTriple(
    triple: [any, any, any],
    create: true,
  ): {
    multiplicity: number | undefined
    map: Map<any, number>
  }
  #getTriple(
    triple: [any, any, any],
    create: false,
  ):
    | {
        multiplicity: number | undefined
        map: Map<any, number>
      }
    | undefined
  #getTriple(
    triple: [any, any, any],
    create = false,
  ):
    | {
        multiplicity: number | undefined
        map: Map<any, number>
      }
    | undefined {
    const [v1, v2, v3] = triple
    let v1Map = this.#tripleMap.get(v1)

    if (!v1Map) {
      if (!create) return undefined
      v1Map = new Map()
      this.#tripleMap.set(v1, v1Map)
    }

    let v2Map = v1Map.get(v2)
    if (!v2Map) {
      if (!create) return undefined
      v2Map = new Map()
      v1Map.set(v2, v2Map)
    }

    return {
      multiplicity: v2Map.get(v3),
      map: v2Map,
    }
  }

  /**
   * Deletes a pair from the pair map.
   */
  #deletePair(pair: [any, any]): void {
    const [v1, v2] = pair
    const map = this.#pairMap.get(v1)
    if (!map) return
    map.delete(v2)
    if (map.size === 0) this.#pairMap.delete(v1)
  }

  /**
   * Deletes a triple from the triple map.
   */
  #deleteTriple(triple: [any, any, any]): void {
    const [v1, v2, v3] = triple
    const v1Map = this.#tripleMap.get(v1)
    if (!v1Map) return
    const v2Map = v1Map.get(v2)
    if (!v2Map) return
    v2Map.delete(v3)
    if (v2Map.size === 0) v1Map.delete(v2)
    if (v1Map.size === 0) this.#tripleMap.delete(v1)
  }

  /**
   * Flattens the pair map into entries.
   */
  #flattenPairMap(): [any, number][] {
    const entries: [any, number][] = []
    for (const [v1, v2Map] of this.#pairMap) {
      for (const [v2, multiplicity] of v2Map) {
        entries.push([[v1, v2], multiplicity])
      }
    }
    return entries
  }

  /**
   * Flattens the triple map into entries.
   */
  #flattenTripleMap(): [any, number][] {
    const entries: [any, number][] = []
    for (const [v1, v2Map] of this.#tripleMap) {
      for (const [v2, v3Map] of v2Map) {
        for (const [v3, multiplicity] of v3Map) {
          entries.push([[v1, [v2, v3]], multiplicity])
        }
      }
    }
    return entries
  }
}
