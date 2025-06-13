import {
  IStreamBuilder,
  DataMessage,
  MessageType,
  KeyValue,
  PipedOperator,
} from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { MultiSet } from '../multiset.js'
import { Antichain, Version } from '../order.js'
import { Index } from '../version-index.js'

interface topKWithPreviousRefOptions<V1, Ref> {
  limit?: number
  offset?: number
  previousRef: (value: V1) => Ref
  nullValue?: Ref  // Optional null value for the first element
}

/**
 * Limits the number of results based on a comparator, with optional offset.
 * This works on a keyed stream, where the key is the first element of the tuple.
 * The ordering is within a key group, i.e. elements are sorted within a key group
 * and the limit + offset is applied to that sorted group.
 * To order the entire stream, key by the same value for all elements such as null.
 * 
 * Each output message is a `[key, [value, previousRef]]` tuple where `previousRef` is
 * a reference to the previous element extracted with the `previousRef` function.
 * The `previousRef` function is provided as an option to the operator.
 * This creates a linked list structure for fine-grained insert/update/delete operations.
 *
 * @param comparator - A function that compares two elements
 * @param options - An optional object containing limit, offset and previousRef properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export class TopKWithPreviousRefOperator<K, V1, Ref> extends UnaryOperator<
  [K, V1 | [V1, Ref]]
> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, [V1, Ref]>()
  #keysTodo = new Map<Version, Set<K>>()
  #comparator: (a: V1, b: V1) => number
  #limit: number
  #offset: number
  #previousRef: (value: V1) => Ref
  #nullValue: Ref | undefined

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, [V1, Ref]]>,
    comparator: (a: V1, b: V1) => number,
    options: topKWithPreviousRefOptions<V1, Ref>,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#comparator = comparator
    this.#limit = options.limit ?? Infinity
    this.#offset = options.offset ?? 0
    this.#previousRef = options.previousRef
    this.#nullValue = options.nullValue
  }

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<[K, V1]>
        for (const [item, multiplicity] of collection.getInner()) {
          const [key, value] = item
          this.#index.addValue(key, version, [value, multiplicity])

          let todoSet = this.#keysTodo.get(version)
          if (!todoSet) {
            todoSet = new Set<K>()
            this.#keysTodo.set(version, todoSet)
          }
          todoSet.add(key)

          // Add key to all join versions
          for (const v2 of this.#index.versions(key)) {
            const joinVersion = version.join(v2)
            let joinTodoSet = this.#keysTodo.get(joinVersion)
            if (!joinTodoSet) {
              joinTodoSet = new Set<K>()
              this.#keysTodo.set(joinVersion, joinTodoSet)
            }
            joinTodoSet.add(key)
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
    const finishedVersions = Array.from(this.#keysTodo.entries())
      .filter(([version]) => !this.inputFrontier().lessEqualVersion(version))
      .sort(([a], [b]) => {
        return a.lessEqual(b) ? -1 : 1
      })

    for (const [version, keys] of finishedVersions) {
      const result: [[K, [V1, Ref]], number][] = []

      for (const key of keys) {
        const curr = this.#index.reconstructAt(key, version)
        const currOut = this.#indexOut.reconstructAt(key, version)

        // Sort the current values
        const consolidated = new MultiSet(curr).consolidate()
        const sortedValues = consolidated
          .getInner()
          .sort((a, b) => this.#comparator(a[0] as V1, b[0] as V1))
          .slice(this.#offset, this.#offset + this.#limit)

        // Create maps for quick lookup
        const currValueMap = new Map<string, V1>()
        const prevOutputMap = new Map<string, [V1, Ref]>()
        const valueToKey = new Map<V1, string>()

        // Process current values
        for (const [value, multiplicity] of sortedValues) {
          if (multiplicity > 0) {
            let valueKey = valueToKey.get(value)
            if (!valueKey) {
              valueKey = JSON.stringify(value)
              valueToKey.set(value, valueKey)
            }
            currValueMap.set(valueKey, value)
          }
        }

        // Process previous output values
        for (const [[value, previousRef], multiplicity] of currOut) {
          if (multiplicity > 0) {
            let valueKey = valueToKey.get(value)
            if (!valueKey) {
              valueKey = JSON.stringify(value)
              valueToKey.set(value, valueKey)
            }
            prevOutputMap.set(valueKey, [value, previousRef])
          }
        }

        // Find values that are no longer in the result
        for (const [valueKey, [value, previousRef]] of prevOutputMap.entries()) {
          if (!currValueMap.has(valueKey)) {
            // Value is no longer in the result, remove it
            result.push([[key, [value, previousRef]], -1])
            this.#indexOut.addValue(key, version, [[value, previousRef], -1])
          }
        }

        // Create the linked list structure
        // Each element gets a reference to the previous element in the sorted order
        let previousValue: V1 | null = null

        for (let i = 0; i < sortedValues.length; i++) {
          const [value, multiplicity] = sortedValues[i]
          if (multiplicity <= 0) continue

          const valueKey = valueToKey.get(value) as string
          
          // Calculate the previous reference
          // The previousRef function extracts a reference from a value
          // For the linked list, we want the reference to the previous element
          let previousRef: Ref
          if (previousValue === null) {
            // First element - use the provided nullValue or throw error if not provided
            if (this.#nullValue !== undefined) {
              previousRef = this.#nullValue
            } else {
              throw new Error('First element in topKWithPreviousRef requires a nullValue to be specified in options')
            }
          } else {
            // Extract reference from the previous element
            previousRef = this.#previousRef(previousValue)
          }

          // Check if this is a new value or if the previousRef has changed
          const existingEntry = prevOutputMap.get(valueKey)

          if (!existingEntry) {
            // New value
            result.push([[key, [value, previousRef]], 1])
            this.#indexOut.addValue(key, version, [[value, previousRef], 1])
          } else if (JSON.stringify(existingEntry[1]) !== JSON.stringify(previousRef)) {
            // Previous reference has changed, remove old entry and add new one
            result.push([[key, existingEntry], -1])
            result.push([[key, [value, previousRef]], 1])
            this.#indexOut.addValue(key, version, [existingEntry, -1])
            this.#indexOut.addValue(key, version, [[value, previousRef], 1])
          }
          // If the value exists and the previousRef hasn't changed, do nothing

          // Update previousValue for the next iteration
          previousValue = value
        }
      }

      if (result.length > 0) {
        this.output.sendData(version, new MultiSet(result))
      }
      this.#keysTodo.delete(version)
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
}

/**
 * Limits the number of results based on a comparator, with optional offset.
 * This works on a keyed stream, where the key is the first element of the tuple.
 * The ordering is within a key group, i.e. elements are sorted within a key group
 * and the limit + offset is applied to that sorted group.
 * To order the entire stream, key by the same value for all elements such as null.
 *
 * Creates a linked list structure where each element contains a reference to the
 * previous element in the sorted order, enabling fine-grained insert/update/delete operations.
 *
 * @param comparator - A function that compares two elements
 * @param options - An object containing limit, offset and previousRef properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function topKWithPreviousRef<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  T,
  Ref,
>(
  comparator: (a: V1, b: V1) => number,
  options: topKWithPreviousRefOptions<V1, Ref>,
): PipedOperator<T, KeyValue<K, [V1, Ref]>> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, [V1, Ref]>> => {
    const output = new StreamBuilder<KeyValue<K, [V1, Ref]>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, [V1, Ref]>>(),
    )
    const operator = new TopKWithPreviousRefOperator<K, V1, Ref>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      output.writer,
      comparator,
      options,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}