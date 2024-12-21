import { Version, Antichain, v } from '../order.js'
import { MultiSet } from '../multiset.js'
import { SQLiteDb, SQLiteStatement } from './database.js'

interface IndexRow {
  key: string
  version: string
  value: string
  multiplicity: number
}

interface InsertParams {
  key: string
  version: string
  value: string
  multiplicity: number
}

interface GetParams {
  key: string
  version: string
}

interface GetAllForKeyParams {
  requestedVersion: string
}

interface JoinResult {
  key: string
  this_version: string
  other_version: string
  this_value: string
  other_value: string
  this_multiplicity: number
  other_multiplicity: number
}

export class SQLIndex<K, V> {
  #db: SQLiteDb
  #tableName: string
  #isTemp: boolean
  #compactionFrontierCache: Antichain | null = null
  #preparedStatements: {
    insert: SQLiteStatement<InsertParams>
    get: SQLiteStatement<GetParams, IndexRow>
    getVersions: SQLiteStatement<[string], { version: string }>
    getAllForKey: SQLiteStatement<[string, GetAllForKeyParams], IndexRow>
    delete: SQLiteStatement<[string]>
    deleteAll: SQLiteStatement
    getForCompaction: SQLiteStatement<[string], IndexRow>
    consolidateVersions: SQLiteStatement<[string, string]>
    deleteZeroMultiplicity: SQLiteStatement
    setCompactionFrontier: SQLiteStatement<[string]>
    getCompactionFrontier: SQLiteStatement<[], { value: string }>
    deleteMeta: SQLiteStatement
    getAllKeys: SQLiteStatement<[], { key: string }>
    getVersionsForKey: SQLiteStatement<[string], { version: string }>
    moveDataToNewVersion: SQLiteStatement<[string, string, string]>
    deleteOldVersionData: SQLiteStatement<[string, string]>
    truncate: SQLiteStatement
    truncateMeta: SQLiteStatement
  }

  // Change cache to store queries instead of statements
  static #appendQueryCache = new Map<string, string>()
  static #joinQueryCache = new Map<string, string>()

  constructor(db: SQLiteDb, name: string, isTemp = false) {
    this.#db = db
    this.#tableName = `index_${name}`
    this.#isTemp = isTemp

    // Create single table with version string directly
    this.#db.exec(`
      CREATE ${isTemp ? 'TEMP' : ''} TABLE IF NOT EXISTS ${this.#tableName} (
        key TEXT NOT NULL,
        version TEXT NOT NULL,
        value TEXT NOT NULL,
        multiplicity INTEGER NOT NULL,
        PRIMARY KEY (key, version, value)
      )
    `)

    this.#db.exec(`
      CREATE ${isTemp ? 'TEMP' : ''} TABLE IF NOT EXISTS ${this.#tableName}_meta (
        key TEXT PRIMARY KEY,
        value TEXT
      )
    `)

    // Create indexes
    if (!isTemp) {
      this.#db.exec(`
        CREATE INDEX IF NOT EXISTS ${this.#tableName}_version_idx 
        ON ${this.#tableName}(version)
      `)
      this.#db.exec(`
        CREATE INDEX IF NOT EXISTS ${this.#tableName}_key_idx 
        ON ${this.#tableName}(key)
      `)
    }

    // Prepare statements
    this.#preparedStatements = {
      insert: this.#db.prepare(`
        INSERT INTO ${this.#tableName} (key, version, value, multiplicity)
        VALUES (@key, @version, @value, @multiplicity)
        ON CONFLICT(key, version, value) DO
          UPDATE SET multiplicity = multiplicity + excluded.multiplicity
      `),

      get: this.#db.prepare(`
        SELECT value, multiplicity 
        FROM ${this.#tableName}
        WHERE key = @key AND version = @version
      `),

      getVersions: this.#db.prepare(`
        SELECT DISTINCT version 
        FROM ${this.#tableName}
        WHERE key = ?
      `),

      getAllForKey: this.#db.prepare(`
        SELECT version, value, multiplicity 
        FROM ${this.#tableName}
        WHERE key = ? AND json_array_length(version) <= json_array_length(@requestedVersion)
      `),

      delete: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
        WHERE version = ?
      `),

      deleteAll: this.#db.prepare(`
        DROP TABLE IF EXISTS ${this.#tableName}
      `),

      getForCompaction: this.#db.prepare(`
        SELECT key, version, value, multiplicity
        FROM ${this.#tableName}
        WHERE version = ?
      `),

      consolidateVersions: this.#db.prepare(`
        WITH consolidated AS (
          SELECT 
            key,
            value,
            CAST(SUM(CAST(multiplicity AS BIGINT)) AS INTEGER) as new_multiplicity
          FROM ${this.#tableName}
          WHERE version = ?
          GROUP BY key, value
        )
        UPDATE ${this.#tableName}
        SET multiplicity = (
          SELECT new_multiplicity
          FROM consolidated c
          WHERE 
            c.key = ${this.#tableName}.key AND
            c.value = ${this.#tableName}.value
        )
        WHERE version = ?
      `),

      deleteZeroMultiplicity: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
        WHERE multiplicity = 0
      `),

      setCompactionFrontier: this.#db.prepare(`
        INSERT OR REPLACE INTO ${this.#tableName}_meta (key, value)
        VALUES ('compaction_frontier', ?)
      `),

      getCompactionFrontier: this.#db.prepare(`
        SELECT value FROM ${this.#tableName}_meta
        WHERE key = 'compaction_frontier'
      `),

      deleteMeta: this.#db.prepare(`
        DROP TABLE IF EXISTS ${this.#tableName}_meta
      `),

      getAllKeys: this.#db.prepare(`
        SELECT DISTINCT key FROM ${this.#tableName}
      `),

      getVersionsForKey: this.#db.prepare(`
        SELECT DISTINCT version
        FROM ${this.#tableName}
        WHERE key = ?
      `),

      moveDataToNewVersion: this.#db.prepare(`
        INSERT INTO ${this.#tableName} (key, version, value, multiplicity)
        SELECT key, ?, value, multiplicity
        FROM ${this.#tableName}
        WHERE key = ? AND version = ?
        ON CONFLICT(key, version, value) DO UPDATE SET
        multiplicity = multiplicity + excluded.multiplicity
      `),

      deleteOldVersionData: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
        WHERE key = ? AND version = ?
      `),

      truncate: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
      `),

      truncateMeta: this.#db.prepare(`
        DELETE FROM ${this.#tableName}_meta
      `),
    }
  }

  get isTemp(): boolean {
    return this.#isTemp
  }

  get tableName(): string {
    return this.#tableName
  }

  getCompactionFrontier(): Antichain | null {
    if (this.#compactionFrontierCache !== null) {
      return this.#compactionFrontierCache
    }

    const frontierRow = this.#preparedStatements.getCompactionFrontier.get()
    if (!frontierRow) return null
    const data = JSON.parse(frontierRow.value) as number[][]
    const frontier = new Antichain(data.map((inner) => v(inner)))

    this.#compactionFrontierCache = frontier
    return frontier
  }

  setCompactionFrontier(frontier: Antichain): void {
    const json = JSON.stringify(frontier.elements.map((v) => v.getInner()))
    this.#preparedStatements.setCompactionFrontier.run(json)
    this.#compactionFrontierCache = frontier
  }

  #validate(requestedVersion: Version | Antichain): boolean {
    const compactionFrontier = this.getCompactionFrontier()
    if (!compactionFrontier) return true

    if (requestedVersion instanceof Antichain) {
      if (!compactionFrontier.lessEqual(requestedVersion)) {
        throw new Error('Invalid version')
      }
    } else if (requestedVersion instanceof Version) {
      if (!compactionFrontier.lessEqualVersion(requestedVersion)) {
        throw new Error('Invalid version')
      }
    }
    return true
  }

  reconstructAt(key: K, requestedVersion: Version): [V, number][] {
    this.#validate(requestedVersion)
    const rows = this.#preparedStatements.getAllForKey.all(
      JSON.stringify(key),
      {
        requestedVersion: requestedVersion.toJSON(),
      },
    )

    const result = rows
      .filter((row) => {
        const version = Version.fromJSON(row.version)
        return version.lessEqual(requestedVersion)
      })
      .map((row) => [JSON.parse(row.value) as V, row.multiplicity])
    return result as [V, number][]
  }

  versions(key: K): Version[] {
    const rows = this.#preparedStatements.getVersions.all(JSON.stringify(key))
    const result = rows.map(({ version }) => Version.fromJSON(version))
    return result
  }

  addValue(key: K, version: Version, value: [V, number]): void {
    this.#validate(version)
    const versionJson = version.toJSON()

    this.#preparedStatements.insert.run({
      key: JSON.stringify(key),
      version: versionJson,
      value: JSON.stringify(value[0]),
      multiplicity: value[1],
    })
  }

  append(other: SQLIndex<K, V>): void {
    const cacheKey = `${this.#tableName}_${other.tableName}`

    let query = SQLIndex.#appendQueryCache.get(cacheKey)
    if (!query) {
      query = `
        INSERT OR REPLACE INTO ${this.#tableName} (key, version, value, multiplicity)
        SELECT 
          o.key,
          o.version,
          o.value,
          COALESCE(t.multiplicity, 0) + o.multiplicity as multiplicity
        FROM ${other.tableName} o
        LEFT JOIN ${this.#tableName} t 
          ON t.key = o.key 
          AND t.version = o.version
          AND t.value = o.value
      `
      SQLIndex.#appendQueryCache.set(cacheKey, query)
    }

    this.#db.prepare(query).run()
  }

  join<V2>(other: SQLIndex<K, V2>): [Version, MultiSet<[K, [V, V2]]>][] {
    const cacheKey = `${this.#tableName}_${other.tableName}`

    let query = SQLIndex.#joinQueryCache.get(cacheKey)
    if (!query) {
      query = `
        SELECT 
          a.key,
          a.version as this_version,
          b.version as other_version,
          a.value as this_value,
          b.value as other_value,
          a.multiplicity as this_multiplicity,
          b.multiplicity as other_multiplicity
        FROM ${this.#tableName} a
        JOIN ${other.tableName} b ON a.key = b.key
      `
      SQLIndex.#joinQueryCache.set(cacheKey, query)
    }

    const results = this.#db.prepare(query).all() as JoinResult[]

    const collections = new Map<string, [K, [V, V2], number][]>()

    for (const row of results) {
      const key = JSON.parse(row.key) as K
      const version1 = Version.fromJSON(row.this_version)
      const version2 = Version.fromJSON(row.other_version)
      const val1 = JSON.parse(row.this_value) as V
      const val2 = JSON.parse(row.other_value) as V2
      const mul1 = row.this_multiplicity
      const mul2 = row.other_multiplicity

      const resultVersion = version1.join(version2)
      const versionKey = resultVersion.toJSON()

      if (!collections.has(versionKey)) {
        collections.set(versionKey, [])
      }

      collections.get(versionKey)!.push([key, [val1, val2], mul1 * mul2])
    }

    const result = Array.from(collections.entries())
      .filter(([_v, c]) => c.length > 0)
      .map(([versionJson, data]) => [
        Version.fromJSON(versionJson),
        new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
      ])

    return result as [Version, MultiSet<[K, [V, V2]]>][]
  }

  destroy(): void {
    this.#preparedStatements.deleteMeta.run()
    this.#preparedStatements.deleteAll.run()
    // Just clear the query caches
    SQLIndex.#appendQueryCache.clear()
    SQLIndex.#joinQueryCache.clear()
    this.#compactionFrontierCache = null
  }

  static clearStatementCaches(): void {
    SQLIndex.#appendQueryCache.clear()
    SQLIndex.#joinQueryCache.clear()
  }

  compact(compactionFrontier: Antichain, keys: K[] = []): void {
    const existingFrontier = this.getCompactionFrontier()
    if (existingFrontier && !existingFrontier.lessEqual(compactionFrontier)) {
      throw new Error('Invalid compaction frontier')
    }

    this.#validate(compactionFrontier)

    // Get all keys if none provided
    const keysToProcess =
      keys.length > 0
        ? keys
        : (() => {
            const rows = this.#preparedStatements.getAllKeys.all()
            return rows.map((row) => JSON.parse(row.key))
          })()

    // Process each key
    for (const key of keysToProcess) {
      // Get versions for this key that need compaction
      const versionsToCompact = this.#preparedStatements.getVersionsForKey
        .all(JSON.stringify(key))
        .map((row) => row.version)

      const toCompact = versionsToCompact.filter((versionJson) => {
        const version = Version.fromJSON(versionJson)
        return !compactionFrontier.lessEqualVersion(version)
      })

      // Track versions that need consolidation
      const toConsolidate = new Set<string>()

      // Process each version that needs compaction
      for (const oldVersionJson of toCompact) {
        const oldVersion = Version.fromJSON(oldVersionJson)
        const newVersion = oldVersion.advanceBy(compactionFrontier)
        const newVersionJson = newVersion.toJSON()

        // Move data to new version
        this.#preparedStatements.moveDataToNewVersion.run(
          newVersionJson,
          JSON.stringify(key),
          oldVersionJson,
        )

        // Delete old version data
        this.#preparedStatements.deleteOldVersionData.run(
          JSON.stringify(key),
          oldVersionJson,
        )

        toConsolidate.add(newVersionJson)
      }

      // Consolidate values for each affected version
      for (const versionJson of toConsolidate) {
        // Consolidate by summing multiplicities
        this.#preparedStatements.consolidateVersions.run(
          versionJson,
          versionJson,
        )

        // Remove entries with zero multiplicity
        this.#preparedStatements.deleteZeroMultiplicity.run()
      }
    }

    this.setCompactionFrontier(compactionFrontier)
  }

  showAll(): { key: K; version: Version; value: V; multiplicity: number }[] {
    const rows = this.#db
      .prepare(`SELECT * FROM ${this.#tableName}`)
      .all() as IndexRow[]
    return rows.map((row) => ({
      key: JSON.parse(row.key),
      version: Version.fromJSON(row.version),
      value: JSON.parse(row.value),
      multiplicity: row.multiplicity,
    }))
  }

  truncate(): void {
    this.#preparedStatements.truncate.run()
    this.#preparedStatements.truncateMeta.run()
    this.#compactionFrontierCache = null
  }
}
