import { Version, Antichain } from './order'
import { MultiSet } from './multiset'
import Database from 'better-sqlite3'

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
  version: string
  joined_value: string
  multiplicity: number
}

interface CompactParams {
  version: string
  newVersion: string
}

export class SQLIndex<K, V> {
  #db: Database.Database
  #tableName: string
  #isTemp: boolean
  #preparedStatements: {
    insert: Database.Statement<InsertParams>
    get: Database.Statement<GetParams, IndexRow>
    getVersions: Database.Statement<[string], { version: string }>
    getAllForKey: Database.Statement<[string, GetAllForKeyParams], IndexRow>
    delete: Database.Statement<[string]>
    deleteAll: Database.Statement
    getForCompaction: Database.Statement<[string], IndexRow>
    updateVersion: Database.Statement<CompactParams>
    consolidateVersions: Database.Statement<[string, string]>
  }

  constructor(db: Database.Database, name: string, isTemp = false) {
    isTemp = false // Temp for debugging
    this.#db = db
    this.#tableName = `index_${name}`
    this.#isTemp = isTemp

    // Create table
    this.#db.exec(`
      CREATE ${isTemp ? 'TEMP' : ''} TABLE IF NOT EXISTS ${this.#tableName} (
        key TEXT NOT NULL,
        version TEXT NOT NULL,
        value TEXT NOT NULL,
        multiplicity INTEGER NOT NULL,
        PRIMARY KEY (key, version, value)
      )
    `)

    // Create index on version for efficient queries
    if (!isTemp) {
      this.#db.exec(`
        CREATE INDEX IF NOT EXISTS ${this.#tableName}_version_idx 
        ON ${this.#tableName}(version)
      `)
    }

    // Prepare statements
    this.#preparedStatements = {
      insert: this.#db.prepare(`
        INSERT INTO ${this.#tableName} (key, version, value, multiplicity)
        VALUES (@key, @version, @value, @multiplicity)
        ON CONFLICT (key, version, value) DO UPDATE SET
        multiplicity = multiplicity + @multiplicity
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

      updateVersion: this.#db.prepare(`
        UPDATE ${this.#tableName}
        SET version = @newVersion
        WHERE version = @version
      `),

      consolidateVersions: this.#db.prepare(`
        WITH consolidated AS (
          SELECT 
            key,
            version,
            value,
            SUM(multiplicity) as multiplicity
          FROM ${this.#tableName}
          WHERE version = ?
          GROUP BY key, version, value
        )
        UPDATE ${this.#tableName}
        SET multiplicity = (
          SELECT multiplicity 
          FROM consolidated 
          WHERE 
            consolidated.key = ${this.#tableName}.key AND
            consolidated.version = ${this.#tableName}.version AND
            consolidated.value = ${this.#tableName}.value
        )
        WHERE version = ?
      `),
    }
  }

  get isTemp(): boolean {
    return this.#isTemp
  }

  get tableName(): string {
    return this.#tableName
  }

  reconstructAt(key: K, requestedVersion: Version): [V, number][] {
    console.log('-> reconstructAt', key, requestedVersion)
    const rows = this.#preparedStatements.getAllForKey.all(
      JSON.stringify(key),
      { requestedVersion: requestedVersion.toJSON() },
    )

    const result = rows
      .filter((row) => {
        const version = Version.fromJSON(row.version)
        return version.lessEqual(requestedVersion)
      })
      .map((row) => [JSON.parse(row.value) as V, row.multiplicity])
    console.log('<- reconstructAt', result)
    return result as [V, number][]
  }

  versions(key: K): Version[] {
    console.log('-> versions', key)
    const rows = this.#preparedStatements.getVersions.all(JSON.stringify(key))
    const result = rows.map(({ version }) => Version.fromJSON(version))
    console.log('<- versions', result)
    return result
  }

  addValue(key: K, version: Version, value: [V, number]): void {
    console.log('-- addValue', key, version, value)
    this.#preparedStatements.insert.run({
      key: JSON.stringify(key),
      version: version.toJSON(),
      value: JSON.stringify(value[0]),
      multiplicity: value[1],
    })
  }

  append(other: SQLIndex<K, V>): void {
    console.log('-- append', other.tableName)

    // Copy all data from the other index into this one
    const insertQuery = `
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
    this.#db.prepare(insertQuery).run()
  }

  join<V2>(other: SQLIndex<K, V2>): [Version, MultiSet<[K, [V, V2]]>][] {
    // Create the join query dynamically with the actual table names
    console.log('-> join', other.tableName)
    const joinQuery = `
      SELECT 
        a.key,
        (
          WITH RECURSIVE numbers(i) AS (
            SELECT 0
            UNION ALL
            SELECT i + 1 FROM numbers 
            WHERE i < json_array_length(a.version) - 1
          )
          SELECT json_group_array(
            MAX(
              json_extract(a.version, '$[' || i || ']'),
              json_extract(b.version, '$[' || i || ']')
            )
          )
          FROM numbers
        ) as version,
        json_array(json(a.value), json(b.value)) as joined_value,
        a.multiplicity * b.multiplicity as multiplicity
      FROM ${this.#tableName} a
      JOIN ${other.tableName} b ON a.key = b.key
      GROUP BY a.key, a.value, b.value
    `

    // Execute the query directly
    const results = this.#db.prepare(joinQuery).all() as JoinResult[]

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
    console.log('<- join', result)
    return result as [Version, MultiSet<[K, [V, V2]]>][]
  }

  destroy(): void {
    console.log('-- destroy')
    this.#preparedStatements.deleteAll.run()
  }

  compact(compactionFrontier: Antichain, keys: K[] = []): void {
    console.log('-- compact', compactionFrontier, keys)

    // Do everything in a single transaction
    this.#db.transaction(() => {
      // First, find all versions that need compaction
      const findVersionsQuery =
        keys.length > 0
          ? `
          SELECT DISTINCT version 
          FROM ${this.#tableName}
          WHERE key IN (${keys.map((k) => `'${JSON.stringify(k)}'`).join(',')})
        `
          : `
          SELECT DISTINCT version 
          FROM ${this.#tableName}
        `

      const versions = this.#db
        .prepare<[], { version: string }>(findVersionsQuery)
        .all()
        .map((row) => Version.fromJSON(row.version))
        .filter((version) => !compactionFrontier.lessEqualVersion(version))

      // For each version that needs compaction
      for (const version of versions) {
        const newVersion = version.advanceBy(compactionFrontier)
        const versionStr = version.toJSON()
        const newVersionStr = newVersion.toJSON()

        // Move data to new version and consolidate in a single query
        const moveAndConsolidateQuery = `
          WITH moved_data AS (
            SELECT 
              key as moved_key,
              '${newVersionStr}' as moved_version,
              value as moved_value,
              SUM(multiplicity) as moved_multiplicity
            FROM ${this.#tableName}
            WHERE version = '${versionStr}'
            GROUP BY key, value
            HAVING SUM(multiplicity) != 0
          )
          INSERT INTO ${this.#tableName} (key, version, value, multiplicity)
          SELECT 
            moved_data.moved_key,
            moved_data.moved_version,
            moved_data.moved_value,
            COALESCE(moved_data.moved_multiplicity, 0) + COALESCE(existing.multiplicity, 0) as multiplicity
          FROM moved_data
          LEFT JOIN ${this.#tableName} existing
            ON existing.key = moved_data.moved_key 
            AND existing.version = moved_data.moved_version
            AND existing.value = moved_data.moved_value
          WHERE COALESCE(moved_data.moved_multiplicity, 0) + COALESCE(existing.multiplicity, 0) != 0
          ON CONFLICT (key, version, value) DO UPDATE SET
            multiplicity = excluded.multiplicity
        `

        // Delete old version after moving data
        const deleteOldVersionQuery = `
          DELETE FROM ${this.#tableName}
          WHERE version = '${versionStr}'
        `

        // Execute the queries
        this.#db.prepare(moveAndConsolidateQuery).run()
        this.#db.prepare(deleteOldVersionQuery).run()
      }
    })()
  }
}
