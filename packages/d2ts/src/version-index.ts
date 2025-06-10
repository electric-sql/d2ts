import { Version, Antichain } from './order.js'
import { MultiSet } from './multiset.js'
import { DefaultMap, chunkedArrayPush, hash } from './utils.js'

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
  #compactionPromise: Promise<void>

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
    this.#compactionPromise = Promise.resolve()
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
      if (out.has(version)) {
        const updatedValues = [...out.get(version)]
        for (const [value, multiplicity] of values) {
          updatedValues.push([value, multiplicity])
        }
        out.set(version, updatedValues)
      } else {
        out.set(version, values)
      }
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
    const collections = new DefaultMap<Version, [K, [V, V2], number][]>(
      () => [],
    )

    // We want to iterate over the smaller of the two indexes to reduce the
    // number of operations we need to do.
    if (this.#inner.size <= other.#inner.size) {
      for (const [key, versions] of this.#inner) {
        if (!other.has(key)) continue
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
      }
    } else {
      for (const [key, otherVersions] of other.entries()) {
        if (!this.has(key)) continue
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
      }
    }

    const result = Array.from(collections.entries())
      .filter(([_v, c]) => c.length > 0)
      .map(([version, data]) => [
        version,
        new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
      ])
    return result as [Version, MultiSet<[K, [V, V2]]>][]
  }

  compact(
    compactionFrontier: Antichain,
    keys: K[] = Array.from(this.#modifiedKeys),
  ): void {
    if (
      this.#compactionFrontier &&
      !this.#compactionFrontier.lessEqual(compactionFrontier)
    ) {
      throw new Error('Invalid compaction frontier')
    }

    this.#validate(compactionFrontier)

    const consolidateValues = (values: [V, number][]): [V, number][] => {
      // Use string representation of values as keys for proper deduplication
      const consolidated = new Map<string | number, [V, number]>()

      for (const [value, multiplicity] of values) {
        const key = hash(value)
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

    for (const key of keys) {
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

  /**
   * Asynchronous version of compact that processes one key at a time and yields to the event loop.
   * This prevents blocking the event loop during compaction of large datasets.
   * Multiple calls to compactAsync are chained to ensure sequential execution.
   * WARNING: Never interleave calls to compactAsync with calls to compact.
   *          Those will not be chained and may interleave, resulting in wrong compaction.
   */
  async compactAsync(
    compactionFrontier: Antichain,
    keys: K[] = Array.from(this.#modifiedKeys),
  ): Promise<void> {
    // Chain this compaction operation to the previous one
    this.#compactionPromise = this.#compactionPromise.then(async () => {
      if (
        this.#compactionFrontier &&
        !this.#compactionFrontier.lessEqual(compactionFrontier)
      ) {
        throw new Error('Invalid compaction frontier')
      }

      this.#validate(compactionFrontier)

      const consolidateValues = (values: [V, number][]): [V, number][] => {
        const consolidated = new Map<string | number, [V, number]>()

        for (const [value, multiplicity] of values) {
          const key = hash(value)
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

      for (const key of keys) {
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

        // Yield to the event loop after processing each key
        await new Promise(resolve => setTimeout(resolve, 0))
      }

      this.#compactionFrontier = compactionFrontier
    })

    return this.#compactionPromise
  }

  keys(): K[] {
    return Array.from(this.#inner.keys())
  }

  has(key: K): boolean {
    return this.#inner.has(key)
  }
}
