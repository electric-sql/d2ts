import { StreamBuilder } from '../../d2.js'
import {
  DataMessage,
  MessageType,
  IStreamBuilder,
  KeyValue,
  PipedOperator,
} from '../../types.js'
import { MultiSet } from '../../multiset.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  BinaryOperator,
} from '../../graph.js'
import { Version, Antichain } from '../../order.js'
import { SQLiteDb } from '../database.js'
import { SQLIndex } from '../version-index.js'
import { JoinType } from '../../operators/join.js'
import { map, concat, negate } from '../../operators/index.js'
import { SQLiteContext } from '../context.js'

export class JoinOperatorSQLite<K, V1, V2> extends BinaryOperator<
  [K, unknown]
> {
  #indexA: SQLIndex<K, V1>
  #indexB: SQLIndex<K, V2>
  #deltaA: SQLIndex<K, V1>
  #deltaB: SQLIndex<K, V2>

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    inputB: DifferenceStreamReader<[K, V2]>,
    output: DifferenceStreamWriter<[K, [V1, V2]]>,
    initialFrontier: Antichain,
    db: SQLiteDb,
  ) {
    super(id, inputA, inputB, output, initialFrontier)
    this.#indexA = new SQLIndex<K, V1>(db, `join_a_${id}`)
    this.#indexB = new SQLIndex<K, V2>(db, `join_b_${id}`)
    this.#deltaA = new SQLIndex<K, V1>(db, `join_delta_a_${id}`, true)
    this.#deltaB = new SQLIndex<K, V2>(db, `join_delta_b_${id}`, true)
  }

  run(): void {
    const deltaA = this.#deltaA
    const deltaB = this.#deltaB

    try {
      // Process input A
      for (const message of this.inputAMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<[K, V1]>
          // Batch the inserts
          const items: [K, Version, [V1, number]][] = []
          for (const [item, multiplicity] of collection.getInner()) {
            const [key, value] = item
            items.push([key, version, [value, multiplicity]])
          }
          deltaA.addValues(items)
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
          // Batch the inserts
          const items: [K, Version, [V2, number]][] = []
          for (const [item, multiplicity] of collection.getInner()) {
            const [key, value] = item
            items.push([key, version, [value, multiplicity]])
          }
          deltaB.addValues(items)
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
      deltaA.truncate()
      deltaB.truncate()
    }
  }
}

/**
 * Joins two input streams
 * Persists state to SQLite
 *
 * @param other - The other stream to join with
 * @param db - Optional SQLite database (can be injected via context)
 * @param type - The type of join to perform
 */
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db?: SQLiteDb,
  type: JoinType = 'inner',
): PipedOperator<T, KeyValue<K, [V1 | null, V2 | null]>> {
  switch (type) {
    case 'inner':
      return innerJoin(other, db) as PipedOperator<T, KeyValue<K, [V1, V2]>>
    case 'anti':
      return antiJoin(other, db) as PipedOperator<T, KeyValue<K, [V1, null]>>
    case 'left':
      return leftJoin(other, db) as PipedOperator<
        T,
        KeyValue<K, [V1, V2 | null]>
      >
    case 'right':
      return rightJoin(other, db)
    case 'full':
      return fullJoin(other, db)
    default:
      throw new Error(`Join type ${type} is invalid`)
  }
}

/**
 * Joins two input streams
 * Persists state to SQLite
 *
 * @param other - The other stream to join with
 * @param db - Optional SQLite database (can be injected via context)
 */
export function innerJoin<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db?: SQLiteDb,
): PipedOperator<T, KeyValue<K, [V1, V2]>> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, [V1, V2]>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for join operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
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
      database,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Performs an anti-join
 *
 * @param other - The other stream to join with
 * @param db - Optional SQLite database (can be injected via context)
 */
export function antiJoin<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db?: SQLiteDb,
): PipedOperator<T, KeyValue<K, [V1, null]>> {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, [V1, null]>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for join operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const matchedLeft = stream.pipe(
      innerJoin(other, database),
      map(([key, [valueLeft, _valueRight]]) => [key, valueLeft]),
    )
    const anti = stream.pipe(
      concat(matchedLeft.pipe(negate())),
      // @ts-ignore TODO: fix this
      map(([key, value]) => [key, [value, null]]),
    )
    return anti as IStreamBuilder<KeyValue<K, [V1, null]>>
  }
}

/**
 * Performs a left join
 *
 * @param other - The other stream to join with
 * @param db - Optional SQLite database (can be injected via context)
 */
export function leftJoin<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db?: SQLiteDb,
): PipedOperator<T, KeyValue<K, [V1, V2 | null]>> {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, [V1, V2 | null]>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for join operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const left = stream
    const right = other
    const inner = left.pipe(innerJoin(right, database))
    const anti = left.pipe(antiJoin(right, database))
    return inner.pipe(concat(anti)) as IStreamBuilder<
      KeyValue<K, [V1, V2 | null]>
    >
  }
}

/**
 * Performs a right join
 *
 * @param other - The other stream to join with
 * @param db - Optional SQLite database (can be injected via context)
 */
export function rightJoin<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db?: SQLiteDb,
): PipedOperator<T, KeyValue<K, [V1 | null, V2]>> {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, [V1 | null, V2]>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for join operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const left = stream as IStreamBuilder<KeyValue<K, V1>>
    const right = other
    const inner = left.pipe(innerJoin(right, database))
    const anti = right.pipe(
      antiJoin(left, database),
      map(([key, [a, b]]) => [key, [b, a]]),
    )
    return inner.pipe(concat(anti)) as IStreamBuilder<
      KeyValue<K, [V1 | null, V2]>
    >
  }
}

/**
 * Performs a full outer join
 *
 * @param other - The other stream to join with
 * @param db - Optional SQLite database (can be injected via context)
 */
export function fullJoin<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db?: SQLiteDb,
): PipedOperator<T, KeyValue<K, [V1 | null, V2 | null]>> {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, [V1 | null, V2 | null]>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for join operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const left = stream as IStreamBuilder<KeyValue<K, V1>>
    const right = other
    const inner = left.pipe(innerJoin(right, database))
    const antiLeft = left.pipe(antiJoin(right, database))
    const antiRight = right.pipe(
      antiJoin(left, database),
      map(([key, [a, b]]) => [key, [b, a]]),
    )
    return inner.pipe(concat(antiLeft), concat(antiRight)) as IStreamBuilder<
      KeyValue<K, [V1 | null, V2 | null]>
    >
  }
}
