import { DataMessage, MessageType } from './types'
import { MultiSet } from './multiset'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from './graph'
import { Version, Antichain } from './order'
import Database from 'better-sqlite3'

// Count of operators to generate unique table names for each operator
// As long as the query is generated in the same order as previous runs, the
// operator count will be correct and the correct table will be used.
let operatorCount = 0

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
  #operatorId: number
  #db: Database.Database
  #preparedStatements: {
    insert: Database.Statement<CollectionParams>
    update: Database.Statement<CollectionParams>
    get: Database.Statement<string, CollectionRow>
    delete: Database.Statement<string>
    getAllVersions: Database.Statement<[], CollectionRow>
  }

  constructor(
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
    db: Database.Database,
  ) {
    const operatorId = operatorCount++

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

    super(inputA, output, inner, initialFrontier)
    this.#operatorId = operatorId

    // Initialize database
    this.#db = db
    this.#db.exec(`
      CREATE TABLE IF NOT EXISTS collections_${this.#operatorId} (
        version TEXT PRIMARY KEY,
        collection TEXT NOT NULL
      )
    `)
    this.#db.exec(`
      CREATE INDEX IF NOT EXISTS collections_${this.#operatorId}_version
      ON collections_${this.#operatorId}(version);
    `)

    // Prepare statements
    this.#preparedStatements = {
      insert: this.#db.prepare(
        `INSERT INTO collections_${this.#operatorId} (version, collection) VALUES (@version, @collection)`,
      ),
      update: this.#db.prepare(
        `UPDATE collections_${this.#operatorId} SET collection = @collection WHERE version = @version`,
      ),
      get: this.#db.prepare(
        `SELECT collection FROM collections_${this.#operatorId} WHERE version = ?`,
      ),
      delete: this.#db.prepare(
        `DELETE FROM collections_${this.#operatorId} WHERE version = ?`,
      ),
      getAllVersions: this.#db.prepare(
        `SELECT version, collection FROM collections_${this.#operatorId}`,
      ),
    }
  }
}

