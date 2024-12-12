import { StreamBuilder } from './d2.js'
import {
  DataMessage,
  MessageType,
  IStreamBuilder,
  PipedOperator,
  KeyValue,
} from './types.js'
import { MultiSet } from './multiset.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
  BinaryOperator,
} from './graph.js'
import { Version, Antichain } from './order.js'
import Database from 'better-sqlite3'
import { SQLIndex } from './version-index-sqlite.js'

interface CollectionRow {
  version: string
  collection: string
}

interface CollectionParams {
  version: string
  collection: string
}

interface KeysTodoRow {
  version: string
  key: string
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
 * @param db - The SQLite database
 */
export function consolidate<T>(db: Database.Database): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new ConsolidateOperatorSQLite<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      stream.graph.frontier(),
      db,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

export class JoinOperatorSQLite<K, V1, V2> extends BinaryOperator<
  [K, unknown]
> {
  #indexA: SQLIndex<K, V1>
  #indexB: SQLIndex<K, V2>
  #db: Database.Database

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    inputB: DifferenceStreamReader<[K, V2]>,
    output: DifferenceStreamWriter<[K, [V1, V2]]>,
    initialFrontier: Antichain,
    db: Database.Database,
  ) {
    super(id, inputA, inputB, output, initialFrontier)
    this.#db = db
    this.#indexA = new SQLIndex<K, V1>(db, `join_a_${id}`)
    this.#indexB = new SQLIndex<K, V2>(db, `join_b_${id}`)
  }

  run(): void {
    const db = this.#db
    const id = this.id

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
}

/**
 * Joins two input streams
 * Persists state to SQLite
 * @param other - The other stream to join with
 * @param db - The SQLite database
 */
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(other: IStreamBuilder<KeyValue<K, V2>>, db: Database.Database) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, [V1, V2]>> => {
    if (stream.graph !== other.graph) {
      throw new Error('Cannot join streams from different graphs')
    }
    const output = new StreamBuilder<KeyValue<K, [V1, V2]>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, [V1, V2]>>(),
    )
    const operator = new JoinOperatorSQLite<K, V1, V2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      other.connectReader() as DifferenceStreamReader<KeyValue<K, V2>>,
      output.writer,
      stream.graph.frontier(),
      db,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
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
    insertKeyTodo: Database.Statement<[string, string]>
    getKeysTodo: Database.Statement<[], KeysTodoRow>
    deleteKeysTodo: Database.Statement<[string]>
    createKeysTodoTable: Database.Statement
    dropKeysTodoTable: Database.Statement
  }
  #f: (values: [V1, number][]) => [V2, number][]

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, V2]>,
    f: (values: [V1, number][]) => [V2, number][],
    initialFrontier: Antichain,
    db: Database.Database,
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
>(f: (values: [V1, number][]) => [R, number][], db: Database.Database) {
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

export class CountOperatorSQLite<K, V> extends ReduceOperatorSQLite<
  K,
  V,
  number
> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, number]>,
    initialFrontier: Antichain,
    db: Database.Database,
  ) {
    const countInner = (vals: [V, number][]): [number, number][] => {
      let count = 0
      for (const [_, diff] of vals) {
        count += diff
      }
      return [[count, 1]]
    }

    super(id, inputA, output, countInner, initialFrontier, db)
  }
}

/**
 * Counts the number of elements by key
 * Persists state to SQLite
 * @param db - The SQLite database
 */
export function count<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V extends T extends KeyValue<K, infer V> ? V : never,
  T,
>(db: Database.Database) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, number>> => {
    const output = new StreamBuilder<KeyValue<K, number>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, number>>(),
    )
    const operator = new CountOperatorSQLite<K, V>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
      output.writer,
      stream.graph.frontier(),
      db,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

export class DistinctOperatorSQLite<K, V> extends ReduceOperatorSQLite<
  K,
  V,
  V
> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, V]>,
    initialFrontier: Antichain,
    db: Database.Database,
  ) {
    const distinctInner = (vals: [V, number][]): [V, number][] => {
      const consolidated = new Map<string, number>()
      const values = new Map<string, V>()
      for (const [val, diff] of vals) {
        const key = JSON.stringify(val)
        consolidated.set(key, (consolidated.get(key) || 0) + diff)
        values.set(key, val)
      }
      return Array.from(consolidated.entries())
        .filter(([_, count]) => count > 0)
        .map(([key, _]) => [values.get(key) as V, 1])
    }

    super(id, inputA, output, distinctInner, initialFrontier, db)
  }
}

/**
 * Removes duplicates by key
 * Persists state to SQLite
 * @param db - The SQLite database
 */
export function distinct<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V extends T extends KeyValue<K, infer V> ? V : never,
  T,
>(db: Database.Database) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, V>> => {
    const output = new StreamBuilder<KeyValue<K, V>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, V>>(),
    )
    const operator = new DistinctOperatorSQLite<K, V>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
      output.writer,
      stream.graph.frontier(),
      db,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
