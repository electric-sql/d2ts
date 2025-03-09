import { StreamBuilder } from '../../d2.js'
import {
  DataMessage,
  MessageType,
  IStreamBuilder,
  PipedOperator,
} from '../../types.js'
import { MultiSet } from '../../multiset.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../../graph.js'
import { Version, Antichain } from '../../order.js'
import { SQLiteDb, SQLiteStatement } from '../database.js'
import { SQLiteContext } from '../context.js'

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
    insert: SQLiteStatement<CollectionParams>
    update: SQLiteStatement<CollectionParams>
    get: SQLiteStatement<[string], CollectionRow>
    delete: SQLiteStatement<[string]>
    getAllVersions: SQLiteStatement<[], CollectionRow>
  }

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
    db: SQLiteDb,
  ) {
    super(id, inputA, output, initialFrontier)

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

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>

        // Get existing collection or create new one
        const existingData = this.#preparedStatements.get.get(version.toJSON())
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
      .filter(({ version }) => !this.inputFrontier().lessEqualVersion(version))

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
}

/**
 * Consolidates the elements in the stream
 * Persists state to SQLite
 *
 * @param db - Optional SQLite database (can be injected via context)
 */
export function consolidate<T>(db?: SQLiteDb): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for consolidate operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new ConsolidateOperatorSQLite<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      stream.graph.frontier(),
      database,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
