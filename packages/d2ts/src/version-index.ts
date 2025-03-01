import { Version, Antichain } from './order.js'
import { MultiSet } from './multiset.js'
import { DefaultMap, chunkedArrayPush } from './utils.js'

type VersionMap<T> = DefaultMap<Version, T[]>
type IndexMap<K, V> = DefaultMap<K, VersionMap<[V, number]>>

export interface IndexType<K, V> {
  reconstructAt(key: K, requestedVersion: Version): [V, number][]
  versions(key: K): Version[]
  addValue(key: K, version: Version, value: [V, number]): void
  append(other: IndexType<K, V>): void
  join<V2>(other: IndexType<K, V2>): [Version, MultiSet<[K, [V, V2]]>][]
  compact(compactionFrontier: Antichain, keys: K[]): void
  keys(): K[]
  has(key: K): boolean
}

/**
 * A map from a difference collection trace's keys -> versions at which
 * the key has nonzero multiplicity -> (value, multiplicities) that changed.
 *
 * Used in operations like join and reduce where the operation needs to
 * exploit the key-value structure of the data to run efficiently.
 *
 * This implementation supports the general case of partially ordered versions.
 */
export class Index<K, V> implements IndexType<K, V> {
  #inner: IndexMap<K, V>
  #compactionFrontier: Antichain | null
  #modifiedKeys: Set<K>

  constructor() {
    this.#inner = new DefaultMap<K, VersionMap<[V, number]>>(
      () => new DefaultMap<Version, [V, number][]>(() => []),
    )
    // #inner is as map of:
    // {
    //   [key]: {
    //     [version]: [value, multiplicity]
    //   }
    // }
    this.#compactionFrontier = null
    this.#modifiedKeys = new Set()
  }

  toString(indent = false): string {
    return `Index(${JSON.stringify(
      [...this.#inner].map(([k, v]) => [k, [...v.entries()]]),
      undefined,
      indent ? '  ' : undefined,
    )})`
  }

  #validate(requestedVersion: Version | Antichain): boolean {
    if (!this.#compactionFrontier) return true

    if (requestedVersion instanceof Antichain) {
      if (!this.#compactionFrontier.lessEqual(requestedVersion)) {
        throw new Error('Invalid version')
      }
    } else if (requestedVersion instanceof Version) {
      if (!this.#compactionFrontier.lessEqualVersion(requestedVersion)) {
        throw new Error('Invalid version')
      }
    }
    return true
  }

  reconstructAt(key: K, requestedVersion: Version): [V, number][] {
    this.#validate(requestedVersion)
    const out: [V, number][] = []
    const versions = this.#inner.get(key)

    for (const [version, values] of versions.entries()) {
      if (version.lessEqual(requestedVersion)) {
        chunkedArrayPush(out, values)
      }
    }

    return out
  }

  get(key: K): VersionMap<[V, number]> {
    if (!this.#compactionFrontier) return this.#inner.get(key)
    // versions may be older than the compaction frontier, so we need to
    // advance them to it. This is due to not rewriting the whole version index
    // to the compaction frontier as part of the compact operation.
    const versions = this.#inner.get(key).entries()
    const out = new DefaultMap<Version, [V, number][]>(() => [])
    for (const [rawVersion, values] of versions) {
      let version = rawVersion
      if (!this.#compactionFrontier.lessEqualVersion(rawVersion)) {
        version = rawVersion.advanceBy(this.#compactionFrontier)
      }
      out.set(version, values)
    }
    return out
  }

  entries(): [K, VersionMap<[V, number]>][] {
    return this.keys().map((key) => [key, this.get(key)])
  }

  versions(key: K): Version[] {
    const result = Array.from(this.get(key).keys())
    return result
  }

  addValue(key: K, version: Version, value: [V, number]): void {
    this.#validate(version)
    const versions = this.#inner.get(key)
    versions.update(version, (values) => {
      values.push(value)
      return values
    })
    this.#modifiedKeys.add(key)
  }

  append(other: Index<K, V>): void {
    for (const [key, versions] of other.entries()) {
      const thisVersions = this.#inner.get(key)
      for (const [version, data] of versions) {
        thisVersions.update(version, (values) => {
          chunkedArrayPush(values, data)
          return values
        })
      }
      this.#modifiedKeys.add(key)
    }
  }

  join<V2>(other: Index<K, V2>): [Version, MultiSet<[K, [V, V2]]>][] {
    // This method is now a wrapper around joinWithType with inner join as default
    return this.joinWithType(other, 'inner') as [
      Version,
      MultiSet<[K, [V, V2]]>,
    ][]
  }

  /**
   * Join this index with another index using a specific join type
   * @param other The other index to join with
   * @param joinType The type of join to perform (inner, left, right, full)
   * @returns An array of [Version, MultiSet<[K, [V | null, V2 | null]]>] tuples
   */
  joinWithType<V2>(
    other: Index<K, V2>,
    joinType: 'inner' | 'left' | 'right' | 'full' = 'inner',
  ): [Version, MultiSet<[K, [V | null, V2 | null]]>][] {
    const collections = new DefaultMap<
      Version,
      [K, [V | null, V2 | null], number][]
    >(() => [])

    // Track which keys have matches in the join
    const matchedKeys = new Set<string>()

    // Process matching keys (inner join part)
    // We want to iterate over the smaller of the two indexes to reduce the
    // number of operations we need to do.
    if (this.#inner.size <= other.#inner.size) {
      for (const [key, versions] of this.#inner) {
        if (other.has(key)) {
          // For matched keys, we create normal inner join results
          matchedKeys.add(JSON.stringify(key))
          const otherVersions = other.get(key)
          for (const [rawVersion1, data1] of versions) {
            const version1 =
              this.#compactionFrontier &&
              this.#compactionFrontier.lessEqualVersion(rawVersion1)
                ? rawVersion1.advanceBy(this.#compactionFrontier)
                : rawVersion1
            for (const [version2, data2] of otherVersions) {
              for (const [val1, mul1] of data1) {
                for (const [val2, mul2] of data2) {
                  const resultVersion = version1.join(version2)
                  collections.update(resultVersion, (existing) => {
                    existing.push([key, [val1, val2], mul1 * mul2])
                    return existing
                  })
                }
              }
            }
          }
        } else if (joinType === 'left' || joinType === 'full') {
          // For left join unmatched records
          this.#processLeftUnmatched(key, versions, collections)
        }
      }
    } else {
      for (const [key, otherVersions] of other.#inner) {
        if (this.has(key)) {
          // For matched keys, we create normal inner join results
          matchedKeys.add(JSON.stringify(key))
          const versions = this.get(key)
          for (const [version2, data2] of otherVersions) {
            for (const [version1, data1] of versions) {
              for (const [val2, mul2] of data2) {
                for (const [val1, mul1] of data1) {
                  const resultVersion = version1.join(version2)
                  collections.update(resultVersion, (existing) => {
                    existing.push([key, [val1, val2], mul1 * mul2])
                    return existing
                  })
                }
              }
            }
          }
        } else if (joinType === 'right' || joinType === 'full') {
          // For right join unmatched records
          this.#processRightUnmatched(key, otherVersions, collections)
        }
      }
    }

    // Handle right-only keys for right and full joins (when iterating over the smaller index first)
    if (
      (joinType === 'right' || joinType === 'full') &&
      this.#inner.size <= other.#inner.size
    ) {
      for (const [key, otherVersions] of other.#inner) {
        const keyJson = JSON.stringify(key)
        if (!matchedKeys.has(keyJson)) {
          this.#processRightUnmatched(key, otherVersions, collections)
        }
      }
    }

    // Handle left-only keys for left and full joins (when iterating over the larger index first)
    if (
      (joinType === 'left' || joinType === 'full') &&
      this.#inner.size > other.#inner.size
    ) {
      for (const [key, versions] of this.#inner) {
        const keyJson = JSON.stringify(key)
        if (!matchedKeys.has(keyJson)) {
          this.#processLeftUnmatched(key, versions, collections)
        }
      }
    }

    const result = Array.from(collections.entries())
      .filter(([_v, c]) => c.length > 0)
      .map(([version, data]) => [
        version,
        new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
      ])
    return result as [Version, MultiSet<[K, [V | null, V2 | null]]>][]
  }

  /**
   * Helper method to process unmatched records for left joins
   */
  #processLeftUnmatched<V2>(
    key: K,
    versions: VersionMap<[V, number]>,
    collections: DefaultMap<Version, [K, [V | null, V2 | null], number][]>,
  ): void {
    for (const [version, data] of versions) {
      for (const [val, mul] of data) {
        if (mul !== 0) {
          collections.update(version, (existing) => {
            existing.push([key, [val, null], mul])
            return existing
          })
        }
      }
    }
  }

  /**
   * Helper method to process unmatched records for right joins
   */
  #processRightUnmatched<V2>(
    key: K,
    versions: VersionMap<[V2, number]>,
    collections: DefaultMap<Version, [K, [V | null, V2 | null], number][]>,
  ): void {
    for (const [version, data] of versions) {
      for (const [val, mul] of data) {
        if (mul !== 0) {
          collections.update(version, (existing) => {
            existing.push([key, [null, val], mul])
            return existing
          })
        }
      }
    }
  }

  compact(compactionFrontier: Antichain, keys: K[] = []): void {
    if (
      this.#compactionFrontier &&
      !this.#compactionFrontier.lessEqual(compactionFrontier)
    ) {
      throw new Error('Invalid compaction frontier')
    }

    this.#validate(compactionFrontier)

    const consolidateValues = (values: [V, number][]): [V, number][] => {
      // Use string representation of values as keys for proper deduplication
      const consolidated = new Map<string, [V, number]>()

      for (const [value, multiplicity] of values) {
        const key = JSON.stringify(value)
        const existing = consolidated.get(key)
        if (existing) {
          consolidated.set(key, [value, existing[1] + multiplicity])
        } else {
          consolidated.set(key, [value, multiplicity])
        }
      }

      return Array.from(consolidated.values()).filter(
        ([_, multiplicity]) => multiplicity !== 0,
      )
    }

    const keysToProcess =
      keys.length > 0 ? keys : Array.from(this.#modifiedKeys)

    for (const key of keysToProcess) {
      const versions = this.#inner.get(key)

      const toCompact = Array.from(versions.keys()).filter(
        (version) => !compactionFrontier.lessEqualVersion(version),
      )

      const toConsolidate = new Set<Version>()

      for (const version of toCompact) {
        const values = versions.get(version)
        versions.delete(version)

        const newVersion = version.advanceBy(compactionFrontier)
        versions.update(newVersion, (existing) => {
          chunkedArrayPush(existing, values)
          return existing
        })
        toConsolidate.add(newVersion)
      }

      for (const version of toConsolidate) {
        const newValues = consolidateValues(versions.get(version))
        if (newValues.length > 0) {
          versions.set(version, newValues)
        } else {
          this.#inner.delete(key)
        }
      }
      this.#modifiedKeys.delete(key)
    }

    this.#compactionFrontier = compactionFrontier
  }

  keys(): K[] {
    return Array.from(this.#inner.keys())
  }

  has(key: K): boolean {
    return this.#inner.has(key)
  }
}
