import { DataMessage, MessageType } from './types'
import { MultiSet } from './multiset'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
  BinaryOperator,
} from './graph'
import { Version, Antichain } from './order'
import Database from 'better-sqlite3'
import { SQLIndex } from './version-index-sqlite'

interface CollectionRow {
  version: string
  collection: string
}

interface CollectionParams {
  version: string
  collection: string
}

/**
 * Operator that consolidates collections at each version, persisting state to SQLite
 */
export class ConsolidateOperatorSQLite<T> extends UnaryOperator<T> {
  #preparedStatements: {
    insert: Database.Statement<CollectionParams>
    update: Database.Statement<CollectionParams>
    get: Database.Statement<string, CollectionRow>
    delete: Database.Statement<string>
    getAllVersions: Database.Statement<[], CollectionRow>
  }

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
    db: Database.Database,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<T>

          // Get existing collection or create new one
          const existingData = this.#preparedStatements.get.get(
            version.toJSON(),
          )
          const existingCollection = existingData
            ? MultiSet.fromJSON(existingData.collection)
            : new MultiSet<T>()

          // Merge collections
          existingCollection.extend(collection)

          // Store updated collection
          if (existingData) {
            this.#preparedStatements.update.run({
              version: version.toJSON(),
              collection: existingCollection.toJSON(),
            })
          } else {
            this.#preparedStatements.insert.run({
              version: version.toJSON(),
              collection: existingCollection.toJSON(),
            })
          }
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.inputFrontier().lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputFrontier(frontier)
        }
      }

      // Find versions that are complete (not covered by input frontier)
      const allVersions = this.#preparedStatements.getAllVersions.all()
      const finishedVersions = allVersions
        .map((row) => ({
          version: Version.fromJSON(row.version),
          collection: MultiSet.fromJSON<T>(row.collection),
        }))
        .filter(
          ({ version }) => !this.inputFrontier().lessEqualVersion(version),
        )

      // Process and remove finished versions
      for (const { version, collection } of finishedVersions) {
        const consolidated = collection.consolidate()
        this.#preparedStatements.delete.run(version.toJSON())
        this.output.sendData(version, consolidated)
      }

      if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
        throw new Error('Invalid frontier state')
      }
      if (this.outputFrontier.lessThan(this.inputFrontier())) {
        this.outputFrontier = this.inputFrontier()
        this.output.sendFrontier(this.outputFrontier)
      }
    }

    super(id, inputA, output, inner, initialFrontier)
    // Initialize database
    db.exec(`
      CREATE TABLE IF NOT EXISTS collections_${this.id} (
        version TEXT PRIMARY KEY,
        collection TEXT NOT NULL
      )
    `)
    db.exec(`
      CREATE INDEX IF NOT EXISTS collections_${this.id}_version
      ON collections_${this.id}(version);
    `)

    // Prepare statements
    this.#preparedStatements = {
      insert: db.prepare(
        `INSERT INTO collections_${this.id} (version, collection) VALUES (@version, @collection)`,
      ),
      update: db.prepare(
        `UPDATE collections_${this.id} SET collection = @collection WHERE version = @version`,
      ),
      get: db.prepare(
        `SELECT collection FROM collections_${this.id} WHERE version = ?`,
      ),
      delete: db.prepare(
        `DELETE FROM collections_${this.id} WHERE version = ?`,
      ),
      getAllVersions: db.prepare(
        `SELECT version, collection FROM collections_${this.id}`,
      ),
    }
  }
}

export class JoinOperatorSQLite<K, V1, V2> extends BinaryOperator<
  [K, unknown]
> {
  #indexA: SQLIndex<K, V1>
  #indexB: SQLIndex<K, V2>

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    inputB: DifferenceStreamReader<[K, V2]>,
    output: DifferenceStreamWriter<[K, [V1, V2]]>,
    initialFrontier: Antichain,
    db: Database.Database,
  ) {
    const inner = () => {
      // Create temporary indexes for this iteration
      const deltaA = new SQLIndex<K, V1>(db, `join_delta_a_${id}`, true)
      const deltaB = new SQLIndex<K, V2>(db, `join_delta_b_${id}`, true)

      try {
        // Process input A
        for (const message of this.inputAMessages()) {
          if (message.type === MessageType.DATA) {
            const { version, collection } = message.data as DataMessage<[K, V1]>
            for (const [item, multiplicity] of collection.getInner()) {
              const [key, value] = item
              deltaA.addValue(key, version, [value, multiplicity])
            }
          } else if (message.type === MessageType.FRONTIER) {
            const frontier = message.data as Antichain
            if (!this.inputAFrontier().lessEqual(frontier)) {
              throw new Error('Invalid frontier update')
            }
            this.setInputAFrontier(frontier)
          }
        }

        // Process input B
        for (const message of this.inputBMessages()) {
          if (message.type === MessageType.DATA) {
            const { version, collection } = message.data as DataMessage<[K, V2]>
            for (const [item, multiplicity] of collection.getInner()) {
              const [key, value] = item
              deltaB.addValue(key, version, [value, multiplicity])
            }
          } else if (message.type === MessageType.FRONTIER) {
            const frontier = message.data as Antichain
            if (!this.inputBFrontier().lessEqual(frontier)) {
              throw new Error('Invalid frontier update')
            }
            this.setInputBFrontier(frontier)
          }
        }

        // Process results
        const results = new Map<Version, MultiSet<[K, [V1, V2]]>>()

        // Join deltaA with existing indexB and collect results
        for (const [version, collection] of deltaA.join(this.#indexB)) {
          const existing = results.get(version) || new MultiSet<[K, [V1, V2]]>()
          existing.extend(collection)
          results.set(version, existing)
        }

        // Append deltaA to indexA
        this.#indexA.append(deltaA)

        // Join indexA with deltaB and collect results
        for (const [version, collection] of this.#indexA.join(deltaB)) {
          const existing = results.get(version) || new MultiSet<[K, [V1, V2]]>()
          existing.extend(collection)
          results.set(version, existing)
        }

        // Send all results
        for (const [version, collection] of results) {
          this.output.sendData(version, collection)
        }

        // Finally append deltaB to indexB
        this.#indexB.append(deltaB)

        // Update frontiers
        const inputFrontier = this.inputAFrontier().meet(this.inputBFrontier())
        if (!this.outputFrontier.lessEqual(inputFrontier)) {
          throw new Error('Invalid frontier state')
        }
        if (this.outputFrontier.lessThan(inputFrontier)) {
          this.outputFrontier = inputFrontier
          this.output.sendFrontier(this.outputFrontier)
          this.#indexA.compact(this.outputFrontier)
          this.#indexB.compact(this.outputFrontier)
        }
      } finally {
        // Clean up temporary indexes
        deltaA.destroy()
        deltaB.destroy()
      }
    }

    super(id, inputA, inputB, output, inner, initialFrontier)

    this.#indexA = new SQLIndex<K, V1>(db, `join_a_${id}`)
    this.#indexB = new SQLIndex<K, V2>(db, `join_b_${id}`)
  }
}
