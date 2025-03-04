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
  deleteAll: SQLiteStatement
  setCompactionFrontier: SQLiteStatement<[string]>
  getCompactionFrontier: SQLiteStatement<[], { value: string }>
  deleteMeta: SQLiteStatement
  getAllKeys: SQLiteStatement<[], { key: string }>
  getVersionsForKey: SQLiteStatement<[string], { version: string }>
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

      deleteAll: this.#db.prepare(`
        DROP TABLE IF EXISTS ${this.#tableName}
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

  /**
   * Returns all keys in the index
   */
  keys(): K[] {
    return this.#preparedStatements.getAllKeys
      .all()
      .map((row) => JSON.parse(row.key) as K)
  }

  /**
   * Checks if a key exists in the index
   */
  has(key: K): boolean {
    const keyCount = this.#db
      .prepare(
        `
      SELECT COUNT(*) as count FROM ${this.#tableName} WHERE key = ?
    `,
      )
      .get(JSON.stringify(key)) as { count: number }

    return keyCount.count > 0
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

  /**
   * Join this index with another index using a specific join type
   * @param other The other index to join with
   * @param joinType The type of join to perform (inner, left, right, full)
   * @returns An array of [Version, MultiSet<[K, [V | null, V2 | null]]>] tuples
   */
  joinWithType<V2>(
    other: SQLIndex<K, V2>,
    joinType: 'inner' | 'left' | 'right' | 'full' = 'inner',
  ): [Version, MultiSet<[K, [V | null, V2 | null]]>][] {
    const joinTypeMap = {
      inner: 'INNER JOIN',
      left: 'LEFT JOIN',
      right: 'RIGHT JOIN',
      full: 'FULL OUTER JOIN',
    }

    const sqlJoinType = joinTypeMap[joinType]
    const cacheKey = `join_${joinType}_${this.#tableName}_${other.tableName}`

    let stmt = this.#statementCache.get(cacheKey)
    if (!stmt) {
      const query = `
        SELECT 
          COALESCE(a.key, b.key) as key,
          a.version as this_version,
          b.version as other_version,
          a.value as this_value,
          b.value as other_value,
          COALESCE(a.multiplicity, 0) as this_multiplicity,
          COALESCE(b.multiplicity, 0) as other_multiplicity
        FROM ${this.#tableName} a
        ${sqlJoinType} ${other.tableName} b ON a.key = b.key
      `
      stmt = this.#db.prepare(query)
      this.#statementCache.set(cacheKey, stmt)
    }

    const results = stmt.all() as JoinResult[]
    const collections = new Map<string, [K, [V | null, V2 | null], number][]>()

    for (const row of results) {
      const key = JSON.parse(row.key) as K

      // Handle null values for versions in outer joins
      const version1 = row.this_version
        ? Version.fromJSON(row.this_version)
        : null
      const version2 = row.other_version
        ? Version.fromJSON(row.other_version)
        : null

      // Handle null values for data in outer joins
      const val1 = row.this_value ? (JSON.parse(row.this_value) as V) : null
      const val2 = row.other_value ? (JSON.parse(row.other_value) as V2) : null

      const mul1 = row.this_multiplicity || 0
      const mul2 = row.other_multiplicity || 0

      // Determine the resulting version
      let resultVersion
      if (version1 && version2) {
        resultVersion = version1.join(version2)
      } else if (version1) {
        resultVersion = version1
      } else if (version2) {
        resultVersion = version2
      } else {
        // This should never happen, but just in case
        continue
      }

      const versionKey = resultVersion.toJSON()

      if (!collections.has(versionKey)) {
        collections.set(versionKey, [])
      }

      // For inner join, both sides must have non-zero multiplicity
      // For outer joins, we only include rows with at least one non-zero multiplicity
      if (
        (joinType === 'inner' && mul1 !== 0 && mul2 !== 0) ||
        (joinType !== 'inner' && (mul1 !== 0 || mul2 !== 0))
      ) {
        // Calculate the multiplicity
        let resultMul
        if (mul1 !== 0 && mul2 !== 0) {
          resultMul = mul1 * mul2
        } else if (mul1 !== 0) {
          resultMul = mul1
        } else {
          resultMul = mul2
        }

        if (resultMul !== 0) {
          collections.get(versionKey)!.push([key, [val1, val2], resultMul])
        }
      }
    }

    const result = Array.from(collections.entries())
      .filter(([_v, c]) => c.length > 0)
      .map(([versionJson, data]) => [
        Version.fromJSON(versionJson),
        new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
      ])

    return result as [Version, MultiSet<[K, [V | null, V2 | null]]>][]
  }

  /**
   * Join this index with multiple other indexes
   * @param others Array of other indexes to join with
   * @param joinType Type of join to perform (inner or left)
   * @returns An array of [Version, MultiSet<[K, (V | null)[]]>] tuples
   */
  joinAll<V2>(
    others: SQLIndex<K, V2>[],
    joinType: 'inner' | 'left' = 'inner',
  ): [Version, MultiSet<[K, (V | null)[]]>][] {
    if (others.length === 0) {
      // If no other indexes, return the current index with array values
      const collections = new Map<string, [K, (V | null)[], number][]>()

      // Use a simple SQL query to get all records in this index
      const stmt = this.#db.prepare(`
        SELECT key, version, value, multiplicity
        FROM ${this.#tableName}
      `)
      const results = stmt.all() as IndexRow[]

      for (const row of results) {
        const key = JSON.parse(row.key) as K
        const version = Version.fromJSON(row.version)
        const val = JSON.parse(row.value) as V
        const mul = row.multiplicity

        if (mul !== 0) {
          const versionKey = version.toJSON()
          if (!collections.has(versionKey)) {
            collections.set(versionKey, [])
          }
          collections.get(versionKey)!.push([key, [val], mul])
        }
      }

      const result = Array.from(collections.entries())
        .filter(([_v, c]) => c.length > 0)
        .map(([versionJson, data]) => [
          Version.fromJSON(versionJson),
          new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
        ])

      return result as [Version, MultiSet<[K, (V | null)[]]>][]
    }

    // Build a SQL query that joins all tables
    const joinTypeSQL = joinType === 'inner' ? 'INNER JOIN' : 'LEFT JOIN'
    const tableAliases = [
      'a',
      ...Array.from(
        { length: others.length },
        (_, i) => String.fromCharCode(98 + i), // b, c, d, ...
      ),
    ]

    // Create a unique cache key for this specific join configuration
    const otherTableNames = others.map((o) => o.tableName).join('_')
    const cacheKey = `join_all_${joinType}_${this.#tableName}_${otherTableNames}`

    let stmt = this.#statementCache.get(cacheKey)
    if (!stmt) {
      // Prepare the SELECT part of the query
      let query = `
        SELECT 
          ${
            joinType === 'inner'
              ? 'a.key'
              : 'COALESCE(a.key, ' +
                tableAliases
                  .slice(1)
                  .map((alias) => `${alias}.key`)
                  .join(', ') +
                ')'
          } as key,
          a.version as base_version,
          a.value as base_value,
          a.multiplicity as base_multiplicity
      `

      // Add columns for each of the other tables
      for (let i = 0; i < others.length; i++) {
        const alias = tableAliases[i + 1]
        query += `,
          ${alias}.version as other${i + 1}_version,
          ${alias}.value as other${i + 1}_value,
          ${alias}.multiplicity as other${i + 1}_multiplicity
        `
      }

      // Start with the base table
      query += `
        FROM ${this.#tableName} a
      `

      // Add joins for each other table
      for (let i = 0; i < others.length; i++) {
        const alias = tableAliases[i + 1]
        query += `
        ${joinTypeSQL} ${others[i].tableName} ${alias} ON a.key = ${alias}.key
        `
      }

      stmt = this.#db.prepare(query)
      this.#statementCache.set(cacheKey, stmt)
    }

    const results = stmt.all() as any[]
    const collections = new Map<string, [K, (V | null)[], number][]>()

    for (const row of results) {
      const key = JSON.parse(row.key) as K

      // Parse base version, value and multiplicity
      const baseVersion = row.base_version
        ? Version.fromJSON(row.base_version)
        : null
      const baseValue = row.base_value
        ? (JSON.parse(row.base_value) as V)
        : null
      const baseMul = row.base_multiplicity || 0

      // For inner join, skip if base is null
      if (joinType === 'inner' && !baseVersion) continue

      // Process each of the other tables
      const otherVersions: (Version | null)[] = []
      const otherValues: (V2 | null)[] = []
      const otherMuls: number[] = []
      let skipRow = false

      for (let i = 0; i < others.length; i++) {
        const versionField = `other${i + 1}_version`
        const valueField = `other${i + 1}_value`
        const mulField = `other${i + 1}_multiplicity`

        const version = row[versionField]
          ? Version.fromJSON(row[versionField])
          : null
        const value = row[valueField]
          ? (JSON.parse(row[valueField]) as V2)
          : null
        const mul = row[mulField] || 0

        // For inner join, skip row if any value is null
        if (joinType === 'inner' && (version === null || value === null)) {
          skipRow = true
          break
        }

        otherVersions.push(version)
        otherValues.push(value)
        otherMuls.push(mul)
      }

      if (skipRow) continue

      // Calculate result version by joining all versions
      let resultVersion = baseVersion!
      for (const version of otherVersions) {
        if (version !== null) {
          resultVersion = resultVersion.join(version)
        }
      }

      // Calculate result multiplicity
      let resultMul = baseMul
      for (const mul of otherMuls) {
        if (mul !== 0) {
          resultMul *= mul
        } else if (joinType === 'inner') {
          resultMul = 0
          break
        }
      }

      // Skip if multiplicity is zero
      if (resultMul === 0) continue

      // Build the values array
      const values: (V | null)[] = [baseValue]
      for (const value of otherValues) {
        values.push(value as unknown as V | null)
      }

      // Store result
      const versionKey = resultVersion.toJSON()
      if (!collections.has(versionKey)) {
        collections.set(versionKey, [])
      }

      collections.get(versionKey)!.push([key, values, resultMul])
    }

    const result = Array.from(collections.entries())
      .filter(([_v, c]) => c.length > 0)
      .map(([versionJson, data]) => [
        Version.fromJSON(versionJson),
        new MultiSet(data.map(([k, v, m]) => [[k, v], m])),
      ])

    return result as [Version, MultiSet<[K, (V | null)[]]>][]
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
