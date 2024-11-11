import { Version, Antichain } from './order'
import { MultiSet } from './multiset'
import { DefaultMap } from './utils'

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

  constructor() {
    this.#inner = new DefaultMap<K, VersionMap<[V, number]>>(
      () => new DefaultMap<Version, [V, number][]>(() => []),
    )
    this.#compactionFrontier = null
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
    console.log('-> reconstructAt', key, requestedVersion)
    this.#validate(requestedVersion)
    const out: [V, number][] = []
    const versions = this.#inner.get(key)

    for (const [version, values] of versions.entries()) {
      if (version.lessEqual(requestedVersion)) {
        out.push(...values)
      }
    }

    console.log('<- reconstructAt', out)
    return out
  }

  versions(key: K): Version[] {
    console.log('-> versions', key)
    const result = Array.from(this.#inner.get(key).keys())
    console.log('<- versions', result)
    return result
  }

  addValue(key: K, version: Version, value: [V, number]): void {
    console.log('-- addValue', key, version, value)
    this.#validate(version)
    const versions = this.#inner.get(key)
    versions.update(version, (values) => {
      values.push(value)
      return values
    })
  }

  append(other: Index<K, V>): void {
    console.log('-- append', other.toString())
    for (const [key, versions] of other.#inner) {
      const thisVersions = this.#inner.get(key)
      for (const [version, data] of versions) {
        thisVersions.update(version, (values) => {
          values.push(...data)
          return values
        })
      }
    }
  }

  join<V2>(other: Index<K, V2>): [Version, MultiSet<[K, [V, V2]]>][] {
    console.log('-> join', other.toString())
    const collections = new DefaultMap<Version, [K, [V, V2], number][]>(
      () => [],
    )

    for (const [key, versions] of this.#inner) {
      if (!other.#inner.has(key)) continue

      const otherVersions = other.#inner.get(key)

      for (const [version1, data1] of versions) {
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

    const result = Array.from(collections.entries())
      .filter(([_v, c]) => c.length > 0)
      .map(([version, data]) => [
        version,
        new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
      ])
    console.log('<- join', result)
    return result as [Version, MultiSet<[K, [V, V2]]>][]
  }

  compact(compactionFrontier: Antichain, keys: K[] = []): void {
    console.log('-- compact', compactionFrontier, keys)
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
      keys.length > 0 ? keys : Array.from(this.#inner.keys())

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
          existing.push(...values)
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
