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
  version_id: number
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
  version: string
  joined_value: string
  multiplicity: number
}

export class SQLIndex<K, V> {
  #db: SQLiteDb
  #tableName: string
  #versionTableName: string
  #isTemp: boolean
  #preparedStatements: {
    insert: SQLiteStatement<InsertParams>
    get: SQLiteStatement<GetParams, IndexRow>
    getVersions: SQLiteStatement<[string], { version: string }>
    getAllForKey: SQLiteStatement<[string, GetAllForKeyParams], IndexRow>
    delete: SQLiteStatement<[string]>
    deleteAll: SQLiteStatement
    deleteAllVersions: SQLiteStatement
    getForCompaction: SQLiteStatement<[string], IndexRow>
    consolidateVersions: SQLiteStatement<[string, string]>
    insertVersion: SQLiteStatement<[string], { id: number }>
    updateVersionMapping: SQLiteStatement<[string, string]>
    deleteZeroMultiplicity: SQLiteStatement
    getVersionId: SQLiteStatement<[string], { id: number }>
    setCompactionFrontier: SQLiteStatement<[string]>
    getCompactionFrontier: SQLiteStatement<[], { value: string }>
    deleteMeta: SQLiteStatement
    getAllKeys: SQLiteStatement<[], { key: string }>
    getVersionsForKey: SQLiteStatement<
      [string],
      { version: string; version_id: number }
    >
    moveDataToNewVersion: SQLiteStatement<[number, string, number]>
    deleteOldVersionData: SQLiteStatement<[string, number]>
    truncate: SQLiteStatement
    truncateMeta: SQLiteStatement
    truncateVersions: SQLiteStatement
  }

  // Cache for frequently used queries
  static #appendQueryCache = new Map<string, string>()
  static #joinQueryCache = new Map<string, string>()

  constructor(db: SQLiteDb, name: string, isTemp = false) {
    this.#db = db
    this.#tableName = `index_${name}`
    this.#versionTableName = `${this.#tableName}_versions`
    this.#isTemp = isTemp

    // Create tables
    this.#db.exec(`
      CREATE ${isTemp ? 'TEMP' : ''} TABLE IF NOT EXISTS ${this.#versionTableName} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        version TEXT NOT NULL
      )
    `)

    this.#db.exec(`
      CREATE ${isTemp ? 'TEMP' : ''} TABLE IF NOT EXISTS ${this.#tableName} (
        key TEXT NOT NULL,
        version_id INTEGER NOT NULL,
        value TEXT NOT NULL,
        multiplicity INTEGER NOT NULL,
        PRIMARY KEY (key, version_id, value),
        FOREIGN KEY (version_id) REFERENCES ${this.#versionTableName}(id)
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
        ON ${this.#tableName}(version_id)
      `)
      this.#db.exec(`
        CREATE INDEX IF NOT EXISTS ${this.#tableName}_key_idx 
        ON ${this.#tableName}(key)
      `)
      this.#db.exec(`
        CREATE INDEX IF NOT EXISTS ${this.#versionTableName}_version_idx 
        ON ${this.#versionTableName}(version)
      `)
    }

    // Prepare statements
    this.#preparedStatements = {
      insert: this.#db.prepare(`
        INSERT INTO ${this.#tableName} (key, version_id, value, multiplicity)
        VALUES (@key, @version_id, @value, @multiplicity)
        ON CONFLICT(key, version_id, value) DO
          UPDATE SET multiplicity = multiplicity + excluded.multiplicity
      `),

      insertVersion: this.#db.prepare(`
        INSERT INTO ${this.#versionTableName} (version)
        VALUES (?)
        RETURNING id
      `),

      get: this.#db.prepare(`
        SELECT value, multiplicity 
        FROM ${this.#tableName} t
        JOIN ${this.#versionTableName} v ON v.id = t.version_id
        WHERE key = @key AND v.version = @version
      `),

      getVersions: this.#db.prepare(`
        SELECT DISTINCT v.version 
        FROM ${this.#tableName} t
        JOIN ${this.#versionTableName} v ON v.id = t.version_id
        WHERE key = ?
      `),

      getAllForKey: this.#db.prepare(`
        SELECT v.version, t.value, t.multiplicity 
        FROM ${this.#tableName} t
        JOIN ${this.#versionTableName} v ON v.id = t.version_id
        WHERE t.key = ? AND json_array_length(v.version) <= json_array_length(@requestedVersion)
      `),

      delete: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
        WHERE version_id IN (
          SELECT id FROM ${this.#versionTableName}
          WHERE version = ?
        )
      `),

      deleteAll: this.#db.prepare(`
        DROP TABLE IF EXISTS ${this.#tableName}
      `),

      deleteAllVersions: this.#db.prepare(`
        DROP TABLE IF EXISTS ${this.#versionTableName}
      `),

      getForCompaction: this.#db.prepare(`
        SELECT t.key, v.version, t.value, t.multiplicity
        FROM ${this.#tableName} t
        JOIN ${this.#versionTableName} v ON v.id = t.version_id
        WHERE v.version = ?
      `),

      consolidateVersions: this.#db.prepare(`
        WITH consolidated AS (
          SELECT 
            t1.key,
            t1.version_id,
            t1.value,
            CAST(SUM(CAST(t1.multiplicity AS BIGINT)) AS INTEGER) as new_multiplicity
          FROM ${this.#tableName} t1
          JOIN ${this.#versionTableName} v1 ON v1.id = t1.version_id
          WHERE v1.version = ?
          GROUP BY t1.key, t1.value, t1.version_id
        )
        UPDATE ${this.#tableName}
        SET multiplicity = (
          SELECT new_multiplicity
          FROM consolidated c
          WHERE 
            c.key = ${this.#tableName}.key AND
            c.value = ${this.#tableName}.value AND
            c.version_id = ${this.#tableName}.version_id
        )
        WHERE version_id IN (
          SELECT id FROM ${this.#versionTableName}
          WHERE version = ?
        )
      `),

      deleteZeroMultiplicity: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
        WHERE multiplicity = 0
      `),

      updateVersionMapping: this.#db.prepare(`
        UPDATE ${this.#versionTableName}
        SET version = ?
        WHERE version = ?
      `),

      getVersionId: this.#db.prepare(`
        SELECT id FROM ${this.#versionTableName}
        WHERE version = ?
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
        SELECT DISTINCT v.version, v.id as version_id
        FROM ${this.#tableName} t
        JOIN ${this.#versionTableName} v ON v.id = t.version_id
        WHERE t.key = ?
      `),

      moveDataToNewVersion: this.#db.prepare(`
        INSERT INTO ${this.#tableName} (key, version_id, value, multiplicity)
        SELECT key, ?, value, multiplicity
        FROM ${this.#tableName}
        WHERE key = ? AND version_id = ?
        ON CONFLICT(key, version_id, value) DO UPDATE SET
        multiplicity = multiplicity + excluded.multiplicity
      `),

      deleteOldVersionData: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
        WHERE key = ? AND version_id = ?
      `),

      truncate: this.#db.prepare(`
        DELETE FROM ${this.#tableName}
      `),

      truncateMeta: this.#db.prepare(`
        DELETE FROM ${this.#tableName}_meta
      `),

      truncateVersions: this.#db.prepare(`
        DELETE FROM ${this.#versionTableName}
      `),
    }
  }

  get isTemp(): boolean {
    return this.#isTemp
  }

  get tableName(): string {
    return this.#tableName
  }

  get versionTableName(): string {
    return this.#versionTableName
  }

  getCompactionFrontier(): Antichain | null {
    const frontierRow = this.#preparedStatements.getCompactionFrontier.get()
    if (!frontierRow) return null
    const data = JSON.parse(frontierRow.value) as number[][]
    return new Antichain(data.map((inner) => v(inner)))
  }

  setCompactionFrontier(frontier: Antichain): void {
    const json = JSON.stringify(frontier.elements.map((v) => v.getInner()))
    this.#preparedStatements.setCompactionFrontier.run(json)
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

    // First try to get existing version id
    let versionRow = this.#preparedStatements.getVersionId.get(versionJson) as
      | { id: number }
      | undefined

    // If version doesn't exist, insert it
    if (!versionRow) {
      versionRow = this.#preparedStatements.insertVersion.get(versionJson) as {
        id: number
      }
    }

    // Insert the actual value
    this.#preparedStatements.insert.run({
      key: JSON.stringify(key),
      version_id: versionRow.id,
      value: JSON.stringify(value[0]),
      multiplicity: value[1],
    })
  }

  append(other: SQLIndex<K, V>): void {
    // First, copy any missing versions from the other version table
    const copyVersionsQuery = `
      INSERT INTO ${this.#versionTableName} (version)
      SELECT DISTINCT o.version 
      FROM ${other.#versionTableName} o
      WHERE NOT EXISTS (
        SELECT 1 FROM ${this.#versionTableName} t 
        WHERE t.version = o.version
      )
    `
    this.#db.prepare(copyVersionsQuery).run()

    // Now use the cached query for the data copy
    const cacheKey = `${this.#tableName}_${other.tableName}`

    let query = SQLIndex.#appendQueryCache.get(cacheKey)
    if (!query) {
      query = `
        INSERT OR REPLACE INTO ${this.#tableName} (key, version_id, value, multiplicity)
        SELECT 
          o.key,
          v2.id as version_id,
          o.value,
          COALESCE(t.multiplicity, 0) + o.multiplicity as multiplicity
        FROM ${other.tableName} o
        JOIN ${other.#versionTableName} v1 ON v1.id = o.version_id
        JOIN ${this.#versionTableName} v2 ON v2.version = v1.version
        LEFT JOIN ${this.#tableName} t 
          ON t.key = o.key 
          AND t.version_id = v2.id
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
          (
            WITH RECURSIVE numbers(i) AS (
              SELECT 0
              UNION ALL
              SELECT i + 1 FROM numbers 
              WHERE i < json_array_length(va.version) - 1
            )
            SELECT json_group_array(
              MAX(
                json_extract(va.version, '$[' || i || ']'),
                json_extract(vb.version, '$[' || i || ']')
              )
            )
            FROM numbers
          ) as version,
          json_array(json(a.value), json(b.value)) as joined_value,
          a.multiplicity * b.multiplicity as multiplicity
        FROM ${this.#tableName} a
        JOIN ${this.#versionTableName} va ON va.id = a.version_id
        JOIN ${other.tableName} b ON a.key = b.key
        JOIN ${other.#versionTableName} vb ON vb.id = b.version_id
        GROUP BY a.key, a.value, b.value
      `
      SQLIndex.#joinQueryCache.set(cacheKey, query)
    }

    const results = this.#db.prepare(query).all() as JoinResult[]

    const versionMap = new Map<string, MultiSet<[K, [V, V2]]>>()

    for (const row of results) {
      const key = JSON.parse(row.key) as K
      const [v1, v2] = JSON.parse(row.joined_value) as [V, V2]

      if (!versionMap.has(row.version)) {
        versionMap.set(row.version, new MultiSet())
      }

      const collection = versionMap.get(row.version)!
      collection.extend([[[key, [v1, v2]], row.multiplicity]])
    }

    const result = Array.from(versionMap.entries()).map(
      ([versionStr, collection]) => [Version.fromJSON(versionStr), collection],
    )
    return result as [Version, MultiSet<[K, [V, V2]]>][]
  }

  destroy(): void {
    this.#preparedStatements.deleteMeta.run()
    this.#preparedStatements.deleteAll.run()
    this.#preparedStatements.deleteAllVersions.run()
    // Clear the query caches
    SQLIndex.#appendQueryCache.clear()
    SQLIndex.#joinQueryCache.clear()
  }

  compact(compactionFrontier: Antichain, keys: K[] = []): void {
    // Check existing frontier
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
            const rows = this.#preparedStatements.getAllKeys.all() as {
              key: string
            }[]
            return rows.map((row) => JSON.parse(row.key))
          })()

    // Process each key
    for (const key of keysToProcess) {
      // Get versions for this key that need compaction
      const versionsToCompact = this.#preparedStatements.getVersionsForKey.all(
        JSON.stringify(key),
      ) as { version: string; version_id: number }[]

      const toCompact = versionsToCompact.filter((row) => {
        const version = Version.fromJSON(row.version)
        return !compactionFrontier.lessEqualVersion(version)
      })

      // Track versions that need consolidation
      const toConsolidate = new Set<string>()

      // Process each version that needs compaction
      for (const versionRow of toCompact) {
        const oldVersion = Version.fromJSON(versionRow.version)
        const newVersion = oldVersion.advanceBy(compactionFrontier)
        const newVersionJson = newVersion.toJSON()

        // Get or create the new version ID
        let newVersionId = this.#preparedStatements.getVersionId.get(
          newVersionJson,
        ) as { id: number } | undefined

        if (!newVersionId) {
          newVersionId = this.#preparedStatements.insertVersion.get(
            newVersionJson,
          ) as {
            id: number
          }
        }

        // Move data to new version, handling conflicts by adding multiplicities
        this.#preparedStatements.moveDataToNewVersion.run(
          newVersionId.id,
          JSON.stringify(key),
          versionRow.version_id,
        )

        // Delete old version data
        this.#preparedStatements.deleteOldVersionData.run(
          JSON.stringify(key),
          versionRow.version_id,
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
      .prepare(
        `
      SELECT i.*, v.version 
      FROM ${this.#tableName} i
      JOIN ${this.#versionTableName} v ON i.version_id = v.id
    `,
      )
      .all()
    return rows as {
      key: K
      version: Version
      value: V
      multiplicity: number
    }[]
  }

  static clearStatementCaches(): void {
    SQLIndex.#appendQueryCache.clear()
    SQLIndex.#joinQueryCache.clear()
  }

  truncate(): void {
    this.#preparedStatements.truncate.run()
    this.#preparedStatements.truncateMeta.run()
    this.#preparedStatements.truncateVersions.run()
  }
}
