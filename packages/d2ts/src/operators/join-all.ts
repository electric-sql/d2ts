import {
  IStreamBuilder,
  PipedOperator,
  DataMessage,
  MessageType,
  KeyValue,
  Message,
} from '../types.js'
import { MultiSet } from '../multiset.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  Operator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { Antichain, Version } from '../order.js'
import { Index } from '../version-index.js'

/**
 * Operator that joins multiple input streams together
 */
export class JoinAllOperator<K, V, V2> extends Operator<
  [K, V] | [K, V2] | [K, (V | null)[]]
> {
  #baseIndex = new Index<K, V>()
  #otherIndexes: Index<K, V2>[] = []
  #joinType: 'inner' | 'left'

  constructor(
    id: number,
    baseReader: DifferenceStreamReader<[K, V]>,
    otherReaders: DifferenceStreamReader<[K, V2]>[],
    output: DifferenceStreamWriter<[K, (V | null)[]]>,
    initialFrontier: Antichain,
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

    this.#joinType = joinType

    // Initialize one index for each other input
    this.#otherIndexes = Array(otherReaders.length)
      .fill(null)
      .map(() => new Index<K, V2>())
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
    const deltaBase = new Index<K, V>()
    const deltaOthers = this.#otherIndexes.map(() => new Index<K, V2>())

    // Process base input
    for (const message of this.baseMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<[K, V]>
        for (const [item, multiplicity] of collection.getInner()) {
          const [key, value] = item
          deltaBase.addValue(key, version, [value, multiplicity])
        }
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
          for (const [item, multiplicity] of collection.getInner()) {
            const [key, value] = item
            deltaIndex.addValue(key, version, [value, multiplicity])
          }
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.otherFrontier(i).lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setOtherFrontier(i, frontier)
        }
      }
    }

    // Add deltas to the main indexes
    this.#baseIndex.append(deltaBase)
    for (let i = 0; i < this.#otherIndexes.length; i++) {
      this.#otherIndexes[i].append(deltaOthers[i])
    }

    // Process results
    const results = new Map<Version, MultiSet<[K, (V | null)[]]>>()
    const processedKeys = new Set<K>()

    // First process changes in the base index with all existing indexes
    if (!deltaBase.keys().length) {
      // Nothing to do if no changes in base
    } else {
      const joinResults = deltaBase.joinAll(this.#otherIndexes, this.#joinType)

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

      const joinResults = this.#baseIndex.joinAll(indexesToJoin, this.#joinType)

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
          const filteredMultiset = new MultiSet<[K, (V | null)[]]>(innerEntries)

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
  }
}

// Overloads for inner join
// 1 input stream
export function joinAll<K, V1, V2>(
  others: [IStreamBuilder<KeyValue<K, V2>>],
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2]>>

// 2 input streams
export function joinAll<K, V1, V2, V3>(
  others: [IStreamBuilder<KeyValue<K, V2>>, IStreamBuilder<KeyValue<K, V3>>],
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3]>>

// 3 input streams
export function joinAll<K, V1, V2, V3, V4>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
  ],
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
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6]>>

// 6 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
  ],
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6, V7]>>

// 7 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
  ],
  joinType: 'inner',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6, V7, V8]>>

// 8 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8, V9>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
    IStreamBuilder<KeyValue<K, V9>>,
  ],
  joinType: 'inner',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2, V3, V4, V5, V6, V7, V8, V9]>
>

// 9 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
    IStreamBuilder<KeyValue<K, V9>>,
    IStreamBuilder<KeyValue<K, V10>>,
  ],
  joinType: 'inner',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2, V3, V4, V5, V6, V7, V8, V9, V10]>
>

// Overloads for left join
// 1 input stream
export function joinAll<K, V1, V2>(
  others: [IStreamBuilder<KeyValue<K, V2>>],
  joinType: 'left',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2 | null]>>

// 2 input streams
export function joinAll<K, V1, V2, V3>(
  others: [IStreamBuilder<KeyValue<K, V2>>, IStreamBuilder<KeyValue<K, V3>>],
  joinType: 'left',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2 | null, V3 | null]>>

// 3 input streams
export function joinAll<K, V1, V2, V3, V4>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
  ],
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
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2 | null, V3 | null, V4 | null, V5 | null, V6 | null]>
>

// 6 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
  ],
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<
    K,
    [V1, V2 | null, V3 | null, V4 | null, V5 | null, V6 | null, V7 | null]
  >
>

// 7 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
  ],
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<
    K,
    [
      V1,
      V2 | null,
      V3 | null,
      V4 | null,
      V5 | null,
      V6 | null,
      V7 | null,
      V8 | null,
    ]
  >
>

// 8 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8, V9>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
    IStreamBuilder<KeyValue<K, V9>>,
  ],
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<
    K,
    [
      V1,
      V2 | null,
      V3 | null,
      V4 | null,
      V5 | null,
      V6 | null,
      V7 | null,
      V8 | null,
      V9 | null,
    ]
  >
>

// 9 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
    IStreamBuilder<KeyValue<K, V9>>,
    IStreamBuilder<KeyValue<K, V10>>,
  ],
  joinType: 'left',
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<
    K,
    [
      V1,
      V2 | null,
      V3 | null,
      V4 | null,
      V5 | null,
      V6 | null,
      V7 | null,
      V8 | null,
      V9 | null,
      V10 | null,
    ]
  >
>

// Default overloads (inner join)
// 1 input stream
export function joinAll<K, V1, V2>(
  others: [IStreamBuilder<KeyValue<K, V2>>],
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2]>>

// 2 input streams
export function joinAll<K, V1, V2, V3>(
  others: [IStreamBuilder<KeyValue<K, V2>>, IStreamBuilder<KeyValue<K, V3>>],
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3]>>

// 3 input streams
export function joinAll<K, V1, V2, V3, V4>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
  ],
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4]>>

// 4 input streams
export function joinAll<K, V1, V2, V3, V4, V5>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
  ],
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
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6]>>

// 6 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
  ],
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6, V7]>>

// 7 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
  ],
): PipedOperator<KeyValue<K, V1>, KeyValue<K, [V1, V2, V3, V4, V5, V6, V7, V8]>>

// 8 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8, V9>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
    IStreamBuilder<KeyValue<K, V9>>,
  ],
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2, V3, V4, V5, V6, V7, V8, V9]>
>

// 9 input streams
export function joinAll<K, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10>(
  others: [
    IStreamBuilder<KeyValue<K, V2>>,
    IStreamBuilder<KeyValue<K, V3>>,
    IStreamBuilder<KeyValue<K, V4>>,
    IStreamBuilder<KeyValue<K, V5>>,
    IStreamBuilder<KeyValue<K, V6>>,
    IStreamBuilder<KeyValue<K, V7>>,
    IStreamBuilder<KeyValue<K, V8>>,
    IStreamBuilder<KeyValue<K, V9>>,
    IStreamBuilder<KeyValue<K, V10>>,
  ],
): PipedOperator<
  KeyValue<K, V1>,
  KeyValue<K, [V1, V2, V3, V4, V5, V6, V7, V8, V9, V10]>
>

// General case for variable number of inputs (will be used when concrete overloads don't match)
export function joinAll<K, V1, V2>(
  others: IStreamBuilder<KeyValue<K, V2>>[],
  joinType?: 'inner' | 'left',
): PipedOperator<KeyValue<K, V1>, KeyValue<K, any[]>>

/**
 * Joins multiple input streams
 * @param others - Array of streams to join with
 * @param joinType - Type of join to perform (inner or left)
 */
export function joinAll<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  others: IStreamBuilder<KeyValue<K, V2>>[],
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

    const operator = new JoinAllOperator<K, V1, V2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      otherReaders,
      output.writer,
      stream.graph.frontier(),
      joinType || 'inner',
    )

    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
