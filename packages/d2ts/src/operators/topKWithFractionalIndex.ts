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
import { generateKeyBetween } from 'fractional-indexing'

interface TopKWithFractionalIndexOptions {
  limit?: number
  offset?: number
}

/**
 * Operator for fractional indexed topK operations
 * This operator maintains fractional indices for sorted elements
 * and only updates indices when elements move position
 */
export class TopKWithFractionalIndexOperator<K, V1> extends UnaryOperator<
  [K, V1 | [V1, string]]
> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, [V1, string]>()
  #keysTodo = new Map<Version, Set<K>>()
  #comparator: (a: V1, b: V1) => number
  #limit: number
  #offset: number

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, [V1, string]]>,
    comparator: (a: V1, b: V1) => number,
    options: TopKWithFractionalIndexOptions,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#comparator = comparator
    this.#limit = options.limit ?? Infinity
    this.#offset = options.offset ?? 0
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
      const result: [[K, [V1, string]], number][] = []

      for (const key of keys) {
        const curr = this.#index.reconstructAt(key, version)
        const currOut = this.#indexOut.reconstructAt(key, version)

        // Sort the current values
        const consolidated = new MultiSet(curr).consolidate()
        const sortedValues = consolidated
          .getInner()
          .sort((a, b) => this.#comparator(a[0] as V1, b[0] as V1))
          .slice(this.#offset, this.#offset + this.#limit)

        // Create a map for quick value lookup with pre-stringified keys
        const currValueMap = new Map<string, V1>()
        const prevOutputMap = new Map<string, [V1, string]>()

        // Pre-stringify all values once
        const valueKeys: string[] = []
        const valueToKey = new Map<V1, string>()

        // Process current values
        for (const [value, multiplicity] of sortedValues) {
          if (multiplicity > 0) {
            // Only stringify each value once and store the result
            let valueKey = valueToKey.get(value)
            if (!valueKey) {
              valueKey = JSON.stringify(value)
              valueToKey.set(value, valueKey)
              valueKeys.push(valueKey)
            }
            currValueMap.set(valueKey, value)
          }
        }

        // Process previous output values
        for (const [[value, index], multiplicity] of currOut) {
          if (multiplicity > 0) {
            // Only stringify each value once and store the result
            let valueKey = valueToKey.get(value)
            if (!valueKey) {
              valueKey = JSON.stringify(value)
              valueToKey.set(value, valueKey)
            }
            prevOutputMap.set(valueKey, [value, index])
          }
        }

        // Find values that are no longer in the result
        for (const [valueKey, [value, index]] of prevOutputMap.entries()) {
          if (!currValueMap.has(valueKey)) {
            // Value is no longer in the result, remove it
            result.push([[key, [value, index]], -1])
            this.#indexOut.addValue(key, version, [[value, index], -1])
          }
        }

        // Process the sorted values and assign fractional indices
        let prevIndex: string | null = null
        let nextIndex: string | null = null
        const newIndices = new Map<string, string>()

        // First pass: reuse existing indices for values that haven't moved
        for (let i = 0; i < sortedValues.length; i++) {
          const [value, _multiplicity] = sortedValues[i]
          // Use the pre-computed valueKey
          const valueKey = valueToKey.get(value) as string

          // Check if this value already has an index
          const existingEntry = prevOutputMap.get(valueKey)

          if (existingEntry) {
            const [_, existingIndex] = existingEntry

            // Check if we need to update the index
            if (i === 0) {
              // First element
              prevIndex = null
              nextIndex =
                i + 1 < sortedValues.length
                  ? newIndices.get(
                      valueToKey.get(sortedValues[i + 1][0]) as string,
                    ) || null
                  : null

              if (nextIndex !== null && existingIndex >= nextIndex) {
                // Need to update index
                const newIndex = generateKeyBetween(prevIndex, nextIndex)
                newIndices.set(valueKey, newIndex)
              } else {
                // Can reuse existing index
                newIndices.set(valueKey, existingIndex)
              }
            } else if (i === sortedValues.length - 1) {
              // Last element
              prevIndex =
                newIndices.get(
                  valueToKey.get(sortedValues[i - 1][0]) as string,
                ) || null
              nextIndex = null

              if (prevIndex !== null && existingIndex <= prevIndex) {
                // Need to update index
                const newIndex = generateKeyBetween(prevIndex, nextIndex)
                newIndices.set(valueKey, newIndex)
              } else {
                // Can reuse existing index
                newIndices.set(valueKey, existingIndex)
              }
            } else {
              // Middle element
              prevIndex =
                newIndices.get(
                  valueToKey.get(sortedValues[i - 1][0]) as string,
                ) || null
              nextIndex =
                i + 1 < sortedValues.length
                  ? newIndices.get(
                      valueToKey.get(sortedValues[i + 1][0]) as string,
                    ) || null
                  : null

              if (
                (prevIndex !== null && existingIndex <= prevIndex) ||
                (nextIndex !== null && existingIndex >= nextIndex)
              ) {
                // Need to update index
                const newIndex = generateKeyBetween(prevIndex, nextIndex)
                newIndices.set(valueKey, newIndex)
              } else {
                // Can reuse existing index
                newIndices.set(valueKey, existingIndex)
              }
            }
          }
        }

        // Second pass: assign new indices for values that don't have one or need to be updated
        for (let i = 0; i < sortedValues.length; i++) {
          const [value, _multiplicity] = sortedValues[i]
          // Use the pre-computed valueKey
          const valueKey = valueToKey.get(value) as string

          if (!newIndices.has(valueKey)) {
            // This value doesn't have an index yet, generate one
            if (i === 0) {
              // First element
              prevIndex = null
              nextIndex =
                i + 1 < sortedValues.length
                  ? newIndices.get(
                      valueToKey.get(sortedValues[i + 1][0]) as string,
                    ) || null
                  : null
            } else if (i === sortedValues.length - 1) {
              // Last element
              prevIndex =
                newIndices.get(
                  valueToKey.get(sortedValues[i - 1][0]) as string,
                ) || null
              nextIndex = null
            } else {
              // Middle element
              prevIndex =
                newIndices.get(
                  valueToKey.get(sortedValues[i - 1][0]) as string,
                ) || null
              nextIndex =
                i + 1 < sortedValues.length
                  ? newIndices.get(
                      valueToKey.get(sortedValues[i + 1][0]) as string,
                    ) || null
                  : null
            }

            const newIndex = generateKeyBetween(prevIndex, nextIndex)
            newIndices.set(valueKey, newIndex)
          }
        }

        // Now create the output with the new indices
        for (let i = 0; i < sortedValues.length; i++) {
          const [value, _multiplicity] = sortedValues[i]
          // Use the pre-computed valueKey
          const valueKey = valueToKey.get(value) as string
          const index = newIndices.get(valueKey)!

          // Check if this is a new value or if the index has changed
          const existingEntry = prevOutputMap.get(valueKey)

          if (!existingEntry) {
            // New value
            result.push([[key, [value, index]], 1])
            this.#indexOut.addValue(key, version, [[value, index], 1])
          } else if (existingEntry[1] !== index) {
            // Index has changed, remove old entry and add new one
            result.push([[key, existingEntry], -1])
            result.push([[key, [value, index]], 1])
            this.#indexOut.addValue(key, version, [existingEntry, -1])
            this.#indexOut.addValue(key, version, [[value, index], 1])
          }
          // If the value exists and the index hasn't changed, do nothing
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
 * Uses fractional indexing to minimize the number of changes when elements move positions.
 * Each element is assigned a fractional index that is lexicographically sortable.
 * When elements move, only the indices of the moved elements are updated, not all elements.
 *
 * @param comparator - A function that compares two elements
 * @param options - An optional object containing limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function topKWithFractionalIndex<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  T,
>(
  comparator: (a: V1, b: V1) => number,
  options?: TopKWithFractionalIndexOptions,
): PipedOperator<T, KeyValue<K, [V1, string]>> {
  const opts = options || {}

  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, [V1, string]>> => {
    const output = new StreamBuilder<KeyValue<K, [V1, string]>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, [V1, string]>>(),
    )
    const operator = new TopKWithFractionalIndexOperator<K, V1>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      output.writer,
      comparator,
      opts,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
