import { Version, Antichain } from './order'
import { MultiSet } from './multiset'

type VersionMap<T> = Map<string, [Version, T[]]> // the key is a string representation of the version in order to avoid duplicates
type IndexMap<K, V> = Map<K, VersionMap<[V, number]>>

/**
 * The implementation of index structures roughly analogous to differential arrangements for manipulating and
 * accessing (key, value) structured data across multiple versions (times).
 */

/**
 * A map from a difference collection trace's keys -> versions at which
 * the key has nonzero multiplicity -> (value, multiplicities) that changed.
 *
 * Used in operations like join and reduce where the operation needs to
 * exploit the key-value structure of the data to run efficiently.
 *
 * This implementation supports the general case of partially ordered versions.
 */
export class Index<K, V> {
  #inner: IndexMap<K, V>
  #compactionFrontier: Antichain | null

  constructor() {
    this.#inner = new Map()
    this.#compactionFrontier = null
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

    if (!versions) return out

    for (const [_, [version, values]] of versions.entries()) {
      if (version.lessEqual(requestedVersion)) {
        out.push(...values)
      }
    }

    return out
  }

  versions(key: K): Version[] {
    const versions = this.#inner.get(key)
    return versions ? Array.from(versions.values()).map(([v]) => v) : []
  }

  addValue(key: K, version: Version, value: [V, number]): void {
    this.#validate(version)

    if (!this.#inner.has(key)) {
      this.#inner.set(key, new Map())
    }

    const versions = this.#inner.get(key)!
    const hash = version.getHash()
    if (!versions.has(hash)) {
      versions.set(hash, [version, []])
    }

    versions.get(hash)![1].push(value)
  }

  append(other: Index<K, V>): void {
    for (const [key, versions] of other.#inner) {
      if (!this.#inner.has(key)) {
        this.#inner.set(key, new Map())
      }

      const thisVersions = this.#inner.get(key)!
      for (const [hash, [version, data]] of versions) {
        if (!thisVersions.has(hash)) {
          thisVersions.set(hash, [version, []])
        }
        thisVersions.get(hash)![1].push(...data)
      }
    }
  }

  join<V2>(other: Index<K, V2>): [Version, MultiSet<[K, [V, V2]]>][] {
    const collections = new Map<Version, [K, [V, V2], number][]>()

    for (const [key, versions] of this.#inner) {
      if (!other.#inner.has(key)) continue

      const otherVersions = other.#inner.get(key)!

      for (const [_, [version1, data1]] of versions) {
        for (const [_, [version2, data2]] of otherVersions) {
          for (const [val1, mul1] of data1) {
            for (const [val2, mul2] of data2) {
              const resultVersion = version1.join(version2)

              if (!collections.has(resultVersion)) {
                collections.set(resultVersion, [])
              }

              collections
                .get(resultVersion)!
                .push([key, [val1, val2], mul1 * mul2])
            }
          }
        }
      }
    }

    return Array.from(collections.entries())
      .filter(([_, c]) => c.length > 0)
      .map(([version, data]) => [
        version,
        new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
      ])
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
      const consolidated = new Map<V, number>()

      for (const [value, multiplicity] of values) {
        const current = consolidated.get(value) || 0
        consolidated.set(value, current + multiplicity)
      }

      return Array.from(consolidated.entries())
        .filter(([_, multiplicity]) => multiplicity !== 0)
        .map(([value, multiplicity]) => [value, multiplicity])
    }

    const keysToProcess =
      keys.length > 0 ? keys : Array.from(this.#inner.keys())

    for (const key of keysToProcess) {
      const versions = this.#inner.get(key)
      if (!versions) continue

      const toCompact = Array.from(versions.entries())
        .filter(
          ([_, [version]]) => !compactionFrontier.lessEqualVersion(version),
        )
        .map(([hash]) => hash)

      const toConsolidate = new Set<string>()

      for (const hash of toCompact) {
        const [version, values] = versions.get(hash)!
        versions.delete(hash)

        const newVersion = version.advanceBy(compactionFrontier)
        const newHash = newVersion.getHash()

        if (!versions.has(newHash)) {
          versions.set(newHash, [newVersion, []])
        }
        versions.get(newHash)![1].push(...values)
        toConsolidate.add(newHash)
      }

      for (const hash of toConsolidate) {
        const [version, values] = versions.get(hash)!
        versions.set(hash, [version, consolidateValues(values)])
      }
    }

    this.#compactionFrontier = compactionFrontier
  }
}
