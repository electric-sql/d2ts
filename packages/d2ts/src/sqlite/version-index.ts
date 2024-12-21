import { Version, Antichain, v } from '../order.js'
import { MultiSet } from '../multiset.js'
import { SQLiteDb, SQLiteStatement } from './database.js'
import { DefaultMap } from '../utils.js'

type VersionMap<T> = DefaultMap<Version, T[]>

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

interface JoinResult {
  key: string
  this_version: string
  other_version: string
  this_value: string
  other_value: string
  this_multiplicity: number
  other_multiplicity: number
}

interface PreparedStatements {
  insert: SQLiteStatement<InsertParams>
  get: SQLiteStatement<GetParams, IndexRow>
  getVersions: SQLiteStatement<[string], { version: string }>
  getAllForKey: SQLiteStatement<[string], IndexRow>
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
  getModifiedKeys: SQLiteStatement<[], { key: string }>
  addModifiedKey: SQLiteStatement<[string]>
  clearModifiedKey: SQLiteStatement<[string]>
  clearAllModifiedKeys: SQLiteStatement
  compactKey: SQLiteStatement<[string, string, string]>
  deleteOldVersionsData: SQLiteStatement<[string, string]>
  getKeysNeedingCompaction: SQLiteStatement<[], { key: string }>
  clearModifiedKeys: SQLiteStatement<[string]>
}

export class SQLIndex<K, V> {
  #db: SQLiteDb
  #tableName: string
  #isTemp: boolean
  #compactionFrontierCache: Antichain | null = null
  #statementCache = new Map<string, SQLiteStatement>()
  #preparedStatements: PreparedStatements

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

    this.#db.exec(`
      CREATE ${isTemp ? 'TEMP' : ''} TABLE IF NOT EXISTS ${this.#tableName}_modified_keys (
        key TEXT PRIMARY KEY
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
        WHERE key = ?
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

      getModifiedKeys: this.#db.prepare(`
        SELECT key FROM ${this.#tableName}_modified_keys
      `),

      addModifiedKey: this.#db.prepare(`
        INSERT OR IGNORE INTO ${this.#tableName}_modified_keys (key)
        VALUES (?)
      `),

      clearModifiedKey: this.#db.prepare(`
        DELETE FROM ${this.#tableName}_modified_keys
        WHERE key = ?
      `),

      clearAllModifiedKeys: this.#db.prepare(`
        DELETE FROM ${this.#tableName}_modified_keys
      `),

      compactKey: this.#db.prepare(`
        WITH moved_data AS (
          -- Move data to new versions and sum multiplicities
          SELECT 
            key,
            ? as new_version,  -- Parameter 1: New version JSON
            value,
            SUM(multiplicity) as multiplicity
          FROM ${this.#tableName}
          WHERE key = ?        -- Parameter 2: Key JSON
          AND version IN (     -- Parameter 3: Old versions JSON array
            SELECT value FROM json_each(?)
          )
          GROUP BY key, value
        )
        INSERT INTO ${this.#tableName} (key, version, value, multiplicity)
        SELECT key, new_version, value, multiplicity 
        FROM moved_data
        WHERE multiplicity != 0
        ON CONFLICT(key, version, value) DO UPDATE SET
          multiplicity = multiplicity + excluded.multiplicity
      `),

      deleteOldVersionsData: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
        WHERE key = ? 
        AND version IN (SELECT value FROM json_each(?))
      `),

      getKeysNeedingCompaction: this.#db.prepare(`
        SELECT key, COUNT(*) as version_count
        FROM (
          SELECT DISTINCT key, version
          FROM ${this.#tableName}
          WHERE key IN (SELECT key FROM ${this.#tableName}_modified_keys)
        )
        GROUP BY key
        HAVING version_count > 1
      `),

      clearModifiedKeys: this.#db.prepare(`
        DELETE FROM ${this.#tableName}_modified_keys
        WHERE key IN (SELECT value FROM json_each(?))
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
    const rows = this.#preparedStatements.getAllForKey.all(JSON.stringify(key))

    const result = rows
      .filter((row) => {
        const version = Version.fromJSON(row.version)
        return version.lessEqual(requestedVersion)
      })
      .map((row) => [JSON.parse(row.value) as V, row.multiplicity])
    return result as [V, number][]
  }

  get(key: K): VersionMap<[V, number]> {
    const rows = this.#preparedStatements.getAllForKey.all(JSON.stringify(key))
    const result = new DefaultMap<Version, [V, number][]>(() => [])
    const compactionFrontier = this.getCompactionFrontier()
    for (const row of rows) {
      let version = Version.fromJSON(row.version)
      if (compactionFrontier && !compactionFrontier.lessEqualVersion(version)) {
        version = version.advanceBy(compactionFrontier)
      }
      result.set(version, [[JSON.parse(row.value) as V, row.multiplicity]])
    }
    return result
  }

  entries(): [K, VersionMap<[V, number]>][] {
    // TODO: This is inefficient, we should use a query to get the entries
    const keys = this.#preparedStatements.getAllKeys
      .all()
      .map((row) => JSON.parse(row.key))
    return keys.map((key) => [key, this.get(key)])
  }

  versions(key: K): Version[] {
    const rows = this.#preparedStatements.getVersions.all(JSON.stringify(key))
    const result = rows.map(({ version }) => Version.fromJSON(version))
    return result
  }

  addValue(key: K, version: Version, value: [V, number]): void {
    this.#validate(version)
    const versionJson = version.toJSON()
    const keyJson = JSON.stringify(key)

    this.#preparedStatements.insert.run({
      key: keyJson,
      version: versionJson,
      value: JSON.stringify(value[0]),
      multiplicity: value[1],
    })

    this.#preparedStatements.addModifiedKey.run(keyJson)
  }

  addValues(items: [K, Version, [V, number]][]): void {
    // SQLite has a limit of 32766 parameters per query
    // Each item uses 4 parameters (key, version, value, multiplicity)
    const BATCH_SIZE = Math.floor(32766 / 4)

    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const batch = items.slice(i, i + BATCH_SIZE)

      // Build the parameterized query for this batch
      const placeholders = batch.map(() => '(?, ?, ?, ?)').join(',')

      const query = `
        INSERT INTO ${this.#tableName} (key, version, value, multiplicity)
        VALUES ${placeholders}
        ON CONFLICT(key, version, value) DO
          UPDATE SET multiplicity = multiplicity + excluded.multiplicity
      `

      // Create flattened parameters array
      const params: (string | number)[] = []
      const modifiedKeys: K[] = []

      batch.forEach(([key, version, [value, multiplicity]]) => {
        this.#validate(version)
        params.push(
          JSON.stringify(key),
          version.toJSON(),
          JSON.stringify(value),
          multiplicity,
        )
        modifiedKeys.push(key)
      })

      // Execute the batch insert
      this.#db.prepare(query).run(params)

      // Track modified keys in batch
      this.addModifiedKeys(modifiedKeys)
    }
  }

  addModifiedKeys(keys: K[]): void {
    // SQLite has a limit of 32766 parameters per query
    const BATCH_SIZE = 32766

    for (let i = 0; i < keys.length; i += BATCH_SIZE) {
      const batch = keys.slice(i, i + BATCH_SIZE)

      const placeholders = batch.map(() => '(?)').join(',')
      const query = `
        INSERT OR IGNORE INTO ${this.#tableName}_modified_keys (key)
        VALUES ${placeholders}
      `

      const params = batch.map((key) => JSON.stringify(key))
      this.#db.prepare(query).run(params)
    }
  }

  append(other: SQLIndex<K, V>): void {
    const cacheKey = `append_${this.#tableName}_${other.tableName}`

    let stmt = this.#statementCache.get(cacheKey)
    if (!stmt) {
      const query = `
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
      stmt = this.#db.prepare(query)
      this.#statementCache.set(cacheKey, stmt)
    }

    stmt.run()

    const modifiedKeysCacheKey = `append_modified_keys_${this.#tableName}_${other.tableName}`
    let modifiedKeysStmt = this.#statementCache.get(modifiedKeysCacheKey)
    if (!modifiedKeysStmt) {
      modifiedKeysStmt = this.#db.prepare(`
        INSERT OR IGNORE INTO ${this.#tableName}_modified_keys (key)
        SELECT DISTINCT key FROM ${other.tableName}
      `)
      this.#statementCache.set(modifiedKeysCacheKey, modifiedKeysStmt)
    }

    modifiedKeysStmt.run()
  }

  join<V2>(other: SQLIndex<K, V2>): [Version, MultiSet<[K, [V, V2]]>][] {
    const cacheKey = `join_${this.#tableName}_${other.tableName}`

    let stmt = this.#statementCache.get(cacheKey)
    if (!stmt) {
      const query = `
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
      stmt = this.#db.prepare(query)
      this.#statementCache.set(cacheKey, stmt)
    }

    const results = stmt.all() as JoinResult[]

    const collections = new Map<string, [K, [V, V2], number][]>()

    for (const row of results) {
      const key = JSON.parse(row.key) as K
      const version1 = Version.fromJSON(row.this_version)
      const version2 = Version.fromJSON(row.other_version)
      const val1 = JSON.parse(row.this_value) as V
      const val2 = JSON.parse(row.other_value) as V2
      const mul1 = row.this_multiplicity
      const mul2 = row.other_multiplicity

      const compactionFrontier1 = this.getCompactionFrontier()
      const compactionFrontier2 = other.getCompactionFrontier()
      if (
        compactionFrontier1 &&
        compactionFrontier1.lessEqualVersion(version1)
      ) {
        version1.advanceBy(compactionFrontier1)
      }
      if (
        compactionFrontier2 &&
        compactionFrontier2.lessEqualVersion(version2)
      ) {
        version2.advanceBy(compactionFrontier2)
      }

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

  compact(compactionFrontier: Antichain, keys: K[] = []): void {
    const existingFrontier = this.getCompactionFrontier()
    if (existingFrontier && !existingFrontier.lessEqual(compactionFrontier)) {
      throw new Error('Invalid compaction frontier')
    }

    this.#validate(compactionFrontier)

    // Get all keys that were modified
    const allKeysToProcess =
      keys.length > 0
        ? keys
        : this.#preparedStatements.getModifiedKeys
            .all()
            .map((row) => JSON.parse(row.key))

    if (allKeysToProcess.length === 0) return

    // Get keys that actually need compaction (have multiple versions)
    const keysToProcessWithMultipleVersions =
      this.#preparedStatements.getKeysNeedingCompaction
        .all()
        .map((row) => JSON.parse(row.key) as K)
        .filter((key) => allKeysToProcess.includes(key))

    // Process each key that needs compaction
    for (const key of keysToProcessWithMultipleVersions) {
      const keyJson = JSON.stringify(key)

      // Get versions for this key that need compaction
      const versionsToCompact = this.#preparedStatements.getVersionsForKey
        .all(keyJson)
        .map((row) => Version.fromJSON(row.version))
        .filter((version) => !compactionFrontier.lessEqualVersion(version))
        .map((version) => version.toJSON())

      // Group versions by their target version after compaction
      const versionGroups = new Map<string, string[]>()
      for (const oldVersionJson of versionsToCompact) {
        const oldVersion = Version.fromJSON(oldVersionJson)
        const newVersion = oldVersion.advanceBy(compactionFrontier)
        const newVersionJson = newVersion.toJSON()

        if (!versionGroups.has(newVersionJson)) {
          versionGroups.set(newVersionJson, [])
        }
        versionGroups.get(newVersionJson)!.push(oldVersionJson)
      }

      // Process each group in a single query
      for (const [newVersionJson, oldVersionJsons] of versionGroups) {
        // Compact all versions in this group to the new version
        this.#preparedStatements.compactKey.run(
          newVersionJson,
          keyJson,
          JSON.stringify(oldVersionJsons),
        )

        // Delete all old versions data at once
        this.#preparedStatements.deleteOldVersionsData.run(
          keyJson,
          JSON.stringify(oldVersionJsons),
        )
      }
    }

    // Clear processed keys from modified keys table in a single query
    if (allKeysToProcess.length > 0) {
      this.#preparedStatements.clearModifiedKeys.run(
        JSON.stringify(allKeysToProcess.map((k) => JSON.stringify(k))),
      )
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
    this.#preparedStatements.clearAllModifiedKeys.run()
    this.#compactionFrontierCache = null
  }

  destroy(): void {
    this.#preparedStatements.deleteMeta.run()
    this.#preparedStatements.deleteAll.run()
    this.#db.exec(`DROP TABLE IF EXISTS ${this.#tableName}_modified_keys`)
    this.#statementCache.clear()
    this.#compactionFrontierCache = null
  }
}
