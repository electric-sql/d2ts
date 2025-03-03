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

export class JoinOperatorSQLite<K, V1, V2> extends BinaryOperator<
  [K, unknown]
> {
  #indexA: SQLIndex<K, V1>
  #indexB: SQLIndex<K, V2>
  #deltaA: SQLIndex<K, V1>
  #deltaB: SQLIndex<K, V2>
  #joinType: JoinType

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    inputB: DifferenceStreamReader<[K, V2]>,
    output: DifferenceStreamWriter<[K, [V1 | null, V2 | null]]>,
    initialFrontier: Antichain,
    db: SQLiteDb,
    joinType: JoinType = 'inner',
  ) {
    super(id, inputA, inputB, output, initialFrontier)
    this.#indexA = new SQLIndex<K, V1>(db, `join_a_${id}`)
    this.#indexB = new SQLIndex<K, V2>(db, `join_b_${id}`)
    this.#deltaA = new SQLIndex<K, V1>(db, `join_delta_a_${id}`, true)
    this.#deltaB = new SQLIndex<K, V2>(db, `join_delta_b_${id}`, true)
    this.#joinType = joinType
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
      const results = new Map<Version, MultiSet<[K, [V1 | null, V2 | null]]>>()

      // Add deltaA to the main index
      this.#indexA.append(deltaA)

      // Add deltaB to the main index
      this.#indexB.append(deltaB)

      // Use SQL native joins to process the data with the appropriate join type
      // This handles inner, left, right, and full joins all in one SQL query
      const joinResults = deltaA.joinWithType(this.#indexB, this.#joinType)

      // Process the results from the SQL join
      for (const [version, multiset] of joinResults) {
        if (!results.has(version)) {
          results.set(version, multiset)
        } else {
          results.get(version)!.extend(multiset.getInner())
        }
      }

      // Also need to check for any joins between existing data and new data
      const otherJoinResults = this.#indexA.joinWithType(deltaB, this.#joinType)

      // Only include results for keys that weren't already matched
      const processedKeys = new Set<K>()

      // Mark keys from first join
      for (const [_version, multiset] of joinResults) {
        for (const [[key, _values], _multiplicity] of multiset.getInner()) {
          processedKeys.add(key)
        }
      }

      // Process other join results, skipping already processed keys
      for (const [version, multiset] of otherJoinResults) {
        const innerEntries: [[K, [V1 | null, V2 | null]], number][] = []

        for (const [[key, value], multiplicity] of multiset.getInner()) {
          if (!processedKeys.has(key)) {
            innerEntries.push([[key, value], multiplicity])
            processedKeys.add(key)
          }
        }

        if (innerEntries.length > 0) {
          const filteredMultiset = new MultiSet<[K, [V1 | null, V2 | null]]>(
            innerEntries,
          )
          if (!results.has(version)) {
            results.set(version, filteredMultiset)
          } else {
            results.get(version)!.extend(filteredMultiset.getInner())
          }
        }
      }

      // Send all results
      for (const [version, collection] of results) {
        this.output.sendData(version, collection)
      }

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

// Overload for inner join - no nulls on either side
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db: SQLiteDb,
  joinType: 'inner',
): PipedOperator<T, KeyValue<K, [V1, V2]>>

// Overload for left join - right side can be null
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db: SQLiteDb,
  joinType: 'left',
): PipedOperator<T, KeyValue<K, [V1, V2 | null]>>

// Overload for right join - left side can be null
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db: SQLiteDb,
  joinType: 'right',
): PipedOperator<T, KeyValue<K, [V1 | null, V2]>>

// Overload for full join - both sides can be null
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db: SQLiteDb,
  joinType: 'full',
): PipedOperator<T, KeyValue<K, [V1 | null, V2 | null]>>

// Default overload for when join type is not specified
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db: SQLiteDb,
): PipedOperator<T, KeyValue<K, [V1, V2]>>

/**
 * Joins two input streams
 * Persists state to SQLite
 * @param other - The other stream to join with
 * @param db - The SQLite database
 * @param joinType - Type of join to perform (inner, left, right, full)
 */
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  db: SQLiteDb,
  joinType: JoinType = 'inner',
) {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, [V1 | null, V2 | null]>> => {
    if (stream.graph !== other.graph) {
      throw new Error('Cannot join streams from different graphs')
    }
    const output = new StreamBuilder<KeyValue<K, [V1 | null, V2 | null]>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, [V1 | null, V2 | null]>>(),
    )
    const operator = new JoinOperatorSQLite<K, V1, V2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      other.connectReader() as DifferenceStreamReader<KeyValue<K, V2>>,
      output.writer,
      stream.graph.frontier(),
      db,
      joinType,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
