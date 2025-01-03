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
  BinaryOperator,
} from '../../graph.js'
import { Version, Antichain } from '../../order.js'
import { SQLiteDb } from '../database.js'
import { SQLIndex } from '../version-index.js'

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
 * @param other - The other stream to join with
 * @param db - The SQLite database
 */
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(other: IStreamBuilder<KeyValue<K, V2>>, db: SQLiteDb) {
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
