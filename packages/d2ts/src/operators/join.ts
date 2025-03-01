import {
  IStreamBuilder,
  PipedOperator,
  DataMessage,
  MessageType,
  KeyValue,
} from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  BinaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { MultiSet } from '../multiset.js'
import { Antichain, Version } from '../order.js'
import { Index } from '../version-index.js'

/**
 * Type of join to perform
 */
export type JoinType = 'inner' | 'left' | 'right' | 'full'

/**
 * Operator that joins two input streams
 */
export class JoinOperator<K, V1, V2> extends BinaryOperator<
  [K, V1] | [K, V2] | [K, [V1 | null, V2 | null]]
> {
  #indexA = new Index<K, V1>()
  #indexB = new Index<K, V2>()
  #joinType: JoinType

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    inputB: DifferenceStreamReader<[K, V2]>,
    output: DifferenceStreamWriter<[K, [V1 | null, V2 | null]]>,
    initialFrontier: Antichain,
    joinType: JoinType = 'inner',
  ) {
    super(id, inputA, inputB, output, initialFrontier)
    this.#joinType = joinType
  }

  run(): void {
    // Process input messages
    const messagesA = this.inputAMessages()
    const messagesB = this.inputBMessages()

    // Track if we have any data messages to process
    let hasDataA = false
    let hasDataB = false

    // Process input A messages
    const deltaA = new Index<K, V1>()
    for (const message of messagesA) {
      if (message.type === MessageType.DATA) {
        hasDataA = true
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

    // Process input B messages
    const deltaB = new Index<K, V2>()
    for (const message of messagesB) {
      if (message.type === MessageType.DATA) {
        hasDataB = true
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

    // Only process joins if we have data to process
    if (hasDataA || hasDataB) {
      // Create a combined index for each input
      const combinedA = new Index<K, V1>()
      combinedA.append(this.#indexA)
      combinedA.append(deltaA)

      const combinedB = new Index<K, V2>()
      combinedB.append(this.#indexB)
      combinedB.append(deltaB)

      // Perform the join using the combined indices
      const joinResults = combinedA.joinWithType(combinedB, this.#joinType)

      // Send the results
      for (const [version, collection] of joinResults) {
        this.output.sendData(version, collection)
      }

      // Update the indices with the deltas
      this.#indexA.append(deltaA)
      this.#indexB.append(deltaB)
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
  }
}

/**
 * Joins two input streams
 * @param other - The other stream to join with
 * @param joinType - Type of join to perform (inner, left, right, full)
 */
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
  joinType: JoinType = 'inner',
): PipedOperator<T, KeyValue<K, [V1 | null, V2 | null]>> {
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
    const operator = new JoinOperator<K, V1, V2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      other.connectReader() as DifferenceStreamReader<KeyValue<K, V2>>,
      output.writer,
      stream.graph.frontier(),
      joinType,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
