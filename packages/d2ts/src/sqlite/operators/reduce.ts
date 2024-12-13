import { StreamBuilder } from '../../d2.js'
import {
  DataMessage,
  MessageType,
  IStreamBuilder,
  KeyValue,
} from '../../types.js'
import { MultiSet } from '../../multiset.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../../graph.js'
import { Version, Antichain } from '../../order.js'
import { SQLiteDb, SQLiteStatement } from '../database.js'
import { SQLIndex } from '../version-index.js'

interface KeysTodoRow {
  version: string
  key: string
}

/**
 * SQLite version of the ReduceOperator
 */
export class ReduceOperatorSQLite<K, V1, V2> extends UnaryOperator<
  [K, V1 | V2]
> {
  #index: SQLIndex<K, V1>
  #indexOut: SQLIndex<K, V2>
  #preparedStatements: {
    insertKeyTodo: SQLiteStatement<[string, string]>
    getKeysTodo: SQLiteStatement<[], KeysTodoRow>
    deleteKeysTodo: SQLiteStatement<[string]>
    createKeysTodoTable: SQLiteStatement
    dropKeysTodoTable: SQLiteStatement
  }
  #f: (values: [V1, number][]) => [V2, number][]

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, V2]>,
    f: (values: [V1, number][]) => [V2, number][],
    initialFrontier: Antichain,
    db: SQLiteDb,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#f = f

    // Initialize indexes
    this.#index = new SQLIndex<K, V1>(db, `reduce_index_${id}`)
    this.#indexOut = new SQLIndex<K, V2>(db, `reduce_index_out_${id}`)

    // Create tables
    db.exec(`
      CREATE TABLE IF NOT EXISTS reduce_keys_todo_${id} (
        version TEXT NOT NULL,
        key TEXT NOT NULL,
        PRIMARY KEY (version, key)
      )
    `)

    // Create indexes for better performance
    db.exec(`
      CREATE INDEX IF NOT EXISTS reduce_keys_todo_${id}_version_idx 
      ON reduce_keys_todo_${id}(version)
    `)

    // Prepare statements
    this.#preparedStatements = {
      createKeysTodoTable: db.prepare(`
        CREATE TABLE IF NOT EXISTS reduce_keys_todo_${id} (
          version TEXT NOT NULL,
          key TEXT NOT NULL,
          PRIMARY KEY (version, key)
        )
      `),
      dropKeysTodoTable: db.prepare(`
        DROP TABLE IF EXISTS reduce_keys_todo_${id}
      `),
      insertKeyTodo: db.prepare(`
        INSERT OR IGNORE INTO reduce_keys_todo_${id} (version, key)
        VALUES (?, ?)
      `),
      getKeysTodo: db.prepare(`
        SELECT version, key FROM reduce_keys_todo_${id}
      `),
      deleteKeysTodo: db.prepare(`
        DELETE FROM reduce_keys_todo_${id}
        WHERE version = ?
      `),
    }
  }

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<[K, V1]>
        for (const [item, multiplicity] of collection.getInner()) {
          const [key, value] = item
          this.#index.addValue(key, version, [value, multiplicity])

          // Add key to todo list for this version
          this.#preparedStatements.insertKeyTodo.run(
            version.toJSON(),
            JSON.stringify(key),
          )

          // Add key to all join versions
          for (const v2 of this.#index.versions(key)) {
            const joinVersion = version.join(v2)
            this.#preparedStatements.insertKeyTodo.run(
              joinVersion.toJSON(),
              JSON.stringify(key),
            )
          }
        }
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
      }
    }

    // Find versions that are complete
    const finishedVersionsRows = this.#preparedStatements.getKeysTodo
      .all()
      .map((row) => ({
        version: Version.fromJSON(row.version),
        key: JSON.parse(row.key) as K,
      }))

    // Group by version
    const finishedVersionsMap = new Map<Version, K[]>()
    for (const { version, key } of finishedVersionsRows) {
      const keys = finishedVersionsMap.get(version) || []
      keys.push(key)
      finishedVersionsMap.set(version, keys)
    }
    const finishedVersions = Array.from(finishedVersionsMap.entries())
      .filter(([version]) => !this.inputFrontier().lessEqualVersion(version))
      .sort((a, b) => (a[0].lessEqual(b[0]) ? -1 : 1))

    for (const [version, keys] of finishedVersions) {
      const result: [[K, V2], number][] = []

      for (const key of keys) {
        const curr = this.#index.reconstructAt(key, version)
        const currOut = this.#indexOut.reconstructAt(key, version)
        const out = this.#f(curr)

        // Calculate delta between current and previous output
        const delta = new Map<string, number>()
        const values = new Map<string, V2>()
        for (const [value, multiplicity] of out) {
          const valueKey = JSON.stringify(value)
          values.set(valueKey, value)
          delta.set(valueKey, (delta.get(valueKey) || 0) + multiplicity)
        }
        for (const [value, multiplicity] of currOut) {
          const valueKey = JSON.stringify(value)
          values.set(valueKey, value)
          delta.set(valueKey, (delta.get(valueKey) || 0) - multiplicity)
        }

        // Add non-zero deltas to result
        for (const [valueKey, multiplicity] of delta) {
          const value = values.get(valueKey)!
          if (multiplicity !== 0) {
            result.push([[key, value], multiplicity])
            this.#indexOut.addValue(key, version, [value, multiplicity])
          }
        }
      }

      if (result.length > 0) {
        this.output.sendData(version, new MultiSet(result))
      }
      this.#preparedStatements.deleteKeysTodo.run(version.toJSON())
    }

    if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(this.outputFrontier)
      this.#index.compact(this.outputFrontier)
      this.#indexOut.compact(this.outputFrontier)
    }
  }

  destroy(): void {
    this.#index.destroy()
    this.#indexOut.destroy()
    this.#preparedStatements.dropKeysTodoTable.run()
  }
}

/**
 * Reduces the elements in the stream by key
 * Persists state to SQLite
 * @param f - The reduction function
 * @param db - The SQLite database
 */
export function reduce<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  R,
  T,
>(f: (values: [V1, number][]) => [R, number][], db: SQLiteDb) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, R>> => {
    const output = new StreamBuilder<KeyValue<K, R>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, R>>(),
    )
    const operator = new ReduceOperatorSQLite<K, V1, R>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      output.writer,
      f,
      stream.graph.frontier(),
      db,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
