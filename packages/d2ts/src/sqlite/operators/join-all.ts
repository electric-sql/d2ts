import { StreamBuilder } from '../../d2.js'
import {
  DataMessage,
  MessageType,
  IStreamBuilder,
  KeyValue,
  PipedOperator,
  Message,
} from '../../types.js'
import { MultiSet } from '../../multiset.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  Operator,
} from '../../graph.js'
import { Version, Antichain } from '../../order.js'
import { SQLiteDb } from '../database.js'
import { SQLIndex } from '../version-index.js'

/**
 * Operator that joins multiple input streams together using SQLite for storage
 */
export class JoinAllOperatorSQLite<K, V, V2> extends Operator<
  [K, V] | [K, V2] | [K, (V | null)[]]
> {
  #baseIndex: SQLIndex<K, V>
  #otherIndexes: SQLIndex<K, V2>[] = []
  #deltaBase: SQLIndex<K, V>
  #deltaOthers: SQLIndex<K, V2>[] = []
  #joinType: 'inner' | 'left'
  #db: SQLiteDb

  constructor(
    id: number,
    baseReader: DifferenceStreamReader<[K, V]>,
    otherReaders: DifferenceStreamReader<[K, V2]>[],
    output: DifferenceStreamWriter<[K, (V | null)[]]>,
    initialFrontier: Antichain,
    db: SQLiteDb,
    joinType: 'inner' | 'left' = 'inner',
  ) {
    // Combine base reader and other readers into a single array of inputs
    const allInputs = [
      baseReader,
      ...otherReaders,
    ] as unknown as DifferenceStreamReader<
      [K, V] | [K, V2] | [K, (V | null)[]]
    >[]
    super(id, allInputs, output, initialFrontier)

    this.#db = db
    this.#joinType = joinType

    // Create SQLite indexes
    this.#baseIndex = new SQLIndex<K, V>(db, `join_all_base_${id}`)
    this.#deltaBase = new SQLIndex<K, V>(db, `join_all_delta_base_${id}`, true)

    // Initialize one index for each other input
    this.#otherIndexes = Array(otherReaders.length)
      .fill(null)
      .map((_, i) => new SQLIndex<K, V2>(db, `join_all_other_${id}_${i}`))

    // Initialize one delta index for each other input
    this.#deltaOthers = Array(otherReaders.length)
      .fill(null)
      .map(
        (_, i) =>
          new SQLIndex<K, V2>(db, `join_all_delta_other_${id}_${i}`, true),
      )
  }

  // Helper method to get the base input messages
  baseMessages(): Message<[K, V]>[] {
    return this.inputs[0].drain() as Message<[K, V]>[]
  }

  // Helper method to get other input messages at index i
  otherMessages(i: number): Message<[K, V2]>[] {
    return this.inputs[i + 1].drain() as Message<[K, V2]>[]
  }

  // Helper method to get the base input frontier
  baseFrontier(): Antichain {
    return this.inputFrontiers[0]
  }

  // Helper method to set the base input frontier
  setBaseFrontier(frontier: Antichain): void {
    this.inputFrontiers[0] = frontier
  }

  // Helper method to get other input frontier at index i
  otherFrontier(i: number): Antichain {
    return this.inputFrontiers[i + 1]
  }

  // Helper method to set other input frontier at index i
  setOtherFrontier(i: number, frontier: Antichain): void {
    this.inputFrontiers[i + 1] = frontier
  }

  run(): void {
    const deltaBase = this.#deltaBase
    const deltaOthers = this.#deltaOthers

    try {
      // Process base input
      for (const message of this.baseMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<[K, V]>
          // Batch the inserts
          const items: [K, Version, [V, number]][] = []
          for (const [item, multiplicity] of collection.getInner()) {
            const [key, value] = item
            items.push([key, version, [value, multiplicity]])
          }
          deltaBase.addValues(items)
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.baseFrontier().lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setBaseFrontier(frontier)
        }
      }

      // Process other inputs
      for (let i = 0; i < this.#otherIndexes.length; i++) {
        const deltaIndex = deltaOthers[i]

        for (const message of this.otherMessages(i)) {
          if (message.type === MessageType.DATA) {
            const { version, collection } = message.data as DataMessage<[K, V2]>
            // Batch the inserts
            const items: [K, Version, [V2, number]][] = []
            for (const [item, multiplicity] of collection.getInner()) {
              const [key, value] = item
              items.push([key, version, [value, multiplicity]])
            }
            deltaIndex.addValues(items)
          } else if (message.type === MessageType.FRONTIER) {
            const frontier = message.data as Antichain
            if (!this.otherFrontier(i).lessEqual(frontier)) {
              throw new Error('Invalid frontier update')
            }
            this.setOtherFrontier(i, frontier)
          }
        }
      }

      // Process results
      const results = new Map<Version, MultiSet<[K, (V | null)[]]>>()
      const processedKeys = new Set<K>()

      // Add deltas to the main indexes
      this.#baseIndex.append(deltaBase)
      for (let i = 0; i < this.#otherIndexes.length; i++) {
        this.#otherIndexes[i].append(deltaOthers[i])
      }

      // First process changes in the base index with all existing indexes
      if (deltaBase.keys().length) {
        const joinResults = deltaBase.joinAll(
          this.#otherIndexes,
          this.#joinType,
        )

        // Add to results
        for (const [version, multiset] of joinResults) {
          for (const [[key, _values], _multiplicity] of multiset.getInner()) {
            processedKeys.add(key)
          }

          if (!results.has(version)) {
            results.set(version, multiset)
          } else {
            results.get(version)!.extend(multiset.getInner())
          }
        }
      }

      // Then process existing base index with changes in other indexes
      for (let i = 0; i < deltaOthers.length; i++) {
        if (!deltaOthers[i].keys().length) {
          // Skip if no changes in this index
          continue
        }

        // Create array of indexes to join with, with the changed one at position i
        const indexesToJoin = [...this.#otherIndexes]
        indexesToJoin[i] = deltaOthers[i]

        const joinResults = this.#baseIndex.joinAll(
          indexesToJoin,
          this.#joinType,
        )

        // Filter out already processed keys
        for (const [version, multiset] of joinResults) {
          const innerEntries: [[K, (V | null)[]], number][] = []

          for (const [[key, value], multiplicity] of multiset.getInner()) {
            if (!processedKeys.has(key)) {
              innerEntries.push([[key, value], multiplicity])
              processedKeys.add(key)
            }
          }

          if (innerEntries.length > 0) {
            const filteredMultiset = new MultiSet<[K, (V | null)[]]>(
              innerEntries,
            )

            if (!results.has(version)) {
              results.set(version, filteredMultiset)
            } else {
              results.get(version)!.extend(filteredMultiset.getInner())
            }
          }
        }
      }

      // Send results
      for (const [version, collection] of results) {
        this.output.sendData(version, collection)
      }

      // Update frontiers - calculate the meet of all input frontiers
      let combinedFrontier = this.baseFrontier()
      for (let i = 0; i < this.#otherIndexes.length; i++) {
        combinedFrontier = combinedFrontier.meet(this.otherFrontier(i))
      }

      if (!this.outputFrontier.lessEqual(combinedFrontier)) {
        throw new Error('Invalid frontier state')
      }

      if (this.outputFrontier.lessThan(combinedFrontier)) {
        this.outputFrontier = combinedFrontier
        this.output.sendFrontier(this.outputFrontier)

        // Compact all indexes
        this.#baseIndex.compact(this.outputFrontier)
        for (const index of this.#otherIndexes) {
          index.compact(this.outputFrontier)
        }
      }
    } finally {
      // Clean up temporary indexes
      deltaBase.truncate()
      for (const deltaIndex of deltaOthers) {
        deltaIndex.truncate()
      }
    }
  }
}

// Overloads for inner join
// 1 input stream
export function joinAll<K, V1, V2>(
  others: [IStreamBuilder<KeyValue<K, V2>>],
  db: SQLiteDb,
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2]>>

// 2 input streams
export function joinAll<K, V1, V2, V3>(
  others: [IStreamBuilder<KeyValue<K, V2>>, IStreamBuilder<KeyValue<K, V3>>],
  db: SQLiteDb,
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3]>>

// 3 input streams
export function joinAll<K, V1, V2, V3, V4>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
  ],
  db: SQLiteDb,
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4]>>

// 4 input streams
export function joinAll<K, V1, V2, V3, V4, V5>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
  ],
  db: SQLiteDb,
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5]>>

// 5 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
  ],
  db: SQLiteDb,
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6]>>

// Overloads for left join
// 1 input stream
export function joinAll<K, V1, V2>(
  others: [IStreamBuilder<KeyValue<K, V2>>],
  db: SQLiteDb,
  joinType: 'left',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2 | null]>>

// 2 input streams
export function joinAll<K, V1, V2, V3>(
  others: [IStreamBuilder<KeyValue<K, V2>>, IStreamBuilder<KeyValue<K, V3>>],
  db: SQLiteDb,
  joinType: 'left',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2 | null, V3 | null]>>

// 3 input streams
export function joinAll<K, V1, V2, V3, V4>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
  ],
  db: SQLiteDb,
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2 | null, V3 | null, V4 | null]>
>

// 4 input streams
export function joinAll<K, V1, V2, V3, V4, V5>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
  ],
  db: SQLiteDb,
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2 | null, V3 | null, V4 | null, V5 | null]>
>

// 5 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
  ],
  db: SQLiteDb,
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2 | null, V3 | null, V4 | null, V5 | null, V6 | null]>
>

// Default overloads (inner join)
// 1 input stream
export function joinAll<K, V1, V2>(
  others: [IStreamBuilder<KeyValue<K, V2>>],
  db: SQLiteDb,
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2]>>

// 2 input streams
export function joinAll<K, V1, V2, V3>(
  others: [IStreamBuilder<KeyValue<K, V2>>, IStreamBuilder<KeyValue<K, V3>>],
  db: SQLiteDb,
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3]>>

// 3 input streams
export function joinAll<K, V1, V2, V3, V4>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
  ],
  db: SQLiteDb,
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4]>>

// 4 input streams
export function joinAll<K, V1, V2, V3, V4, V5>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
  ],
  db: SQLiteDb,
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5]>>

// 5 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
  ],
  db: SQLiteDb,
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6]>>

// General case for variable number of inputs (will be used when concrete overloads don't match)
export function joinAll<K, V1, V2>(
  others: IStreamBuilder<KeyValue<K, V2>>[],
  db: SQLiteDb,
  joinType?: 'inner' | 'left',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, any[]>>

/**
 * Joins multiple input streams with SQLite storage
 * @param others - Array of streams to join with
 * @param db - SQLite database to use for storage
 * @param joinType - Type of join to perform (inner or left)
 */
export function joinAll<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  others: IStreamBuilder<KeyValue<K, V2>>[],
  db: SQLiteDb,
  joinType?: 'inner' | 'left',
): PipedOperator<T, KeyValue<K, (V1 | null)[]>> {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, (V1 | null)[]>> => {
    // Check if all streams are from the same graph
    if (others.some((other) => stream.graph !== other.graph)) {
      throw new Error('Cannot join streams from different graphs')
    }

    const output = new StreamBuilder<KeyValue<K, (V1 | null)[]>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, (V1 | null)[]>>(),
    )

    const otherReaders = others.map(
      (other) =>
        other.connectReader() as DifferenceStreamReader<KeyValue<K, V2>>,
    )

    const operator = new JoinAllOperatorSQLite<K, V1, V2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      otherReaders,
      output.writer,
      stream.graph.frontier(),
      db,
      joinType || 'inner',
    )

    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
