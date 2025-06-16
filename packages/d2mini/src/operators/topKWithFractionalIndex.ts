import { IStreamBuilder, KeyValue, PipedOperator } from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { MultiSet } from '../multiset.js'
import { Index } from '../indexes.js'
import { generateKeyBetween } from 'fractional-indexing'
import { hash } from '../utils.js'

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
  [K, V1],
  [K, [V1, string]]
> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, [V1, string]>()
  #comparator: (a: V1, b: V1) => number
  #limit: number
  #offset: number

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, [V1, string]]>,
    comparator: (a: V1, b: V1) => number,
    options: TopKWithFractionalIndexOptions,
  ) {
    super(id, inputA, output)
    this.#comparator = comparator
    this.#limit = options.limit ?? Infinity
    this.#offset = options.offset ?? 0
  }

  run(): void {
    const keysTodo = new Set<K>()

    for (const message of this.inputMessages()) {
      for (const [item, multiplicity] of message.getInner()) {
        const [key, value] = item
        this.#index.addValue(key, [value, multiplicity])
        keysTodo.add(key)
      }
    }

    const result: [[K, [V1, string]], number][] = []

    for (const key of keysTodo) {
      const curr = this.#index.reconstruct(key)
      const currOut = this.#indexOut.reconstruct(key)

      // Sort the current values
      const consolidated = new MultiSet(curr).consolidate()
      const sortedValues = consolidated
        .getInner()
        .sort((a, b) => this.#comparator(a[0] as V1, b[0] as V1))
        .slice(this.#offset, this.#offset + this.#limit)

      // Create a map for quick value lookup with pre-stringified keys
      const currValueMap = new Map<string | number, V1>()
      const prevOutputMap = new Map<string | number, [V1, string]>()

      // Pre-stringify all values once
      const valueKeys: (string | number)[] = []
      const valueToKey = new Map<V1, string | number>()

      // Process current values
      for (const [value, multiplicity] of sortedValues) {
        if (multiplicity > 0) {
          // Only stringify each value once and store the result
          let valueKey = valueToKey.get(value as V1)
          if (!valueKey) {
            valueKey = hash(value)
            valueToKey.set(value as V1, valueKey)
            valueKeys.push(valueKey)
          }
          currValueMap.set(valueKey, value as V1)
        }
      }

      // Process previous output values
      for (const [[value, index], multiplicity] of currOut) {
        if (multiplicity > 0) {
          // Only stringify each value once and store the result
          let valueKey = valueToKey.get(value as V1)
          if (!valueKey) {
            valueKey = hash(value)
            valueToKey.set(value as V1, valueKey)
          }
          prevOutputMap.set(valueKey, [value as V1, index as string])
        }
      }

      // Find values that are no longer in the result
      for (const [valueKey, [value, index]] of prevOutputMap.entries()) {
        if (!currValueMap.has(valueKey)) {
          // Value is no longer in the result, remove it
          result.push([[key, [value, index]], -1])
          this.#indexOut.addValue(key, [[value, index], -1])
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
        const valueKey = valueToKey.get(value as V1) as string

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
                    valueToKey.get(sortedValues[i + 1][0] as V1) as string,
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
                valueToKey.get(sortedValues[i - 1][0] as V1) as string,
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
                valueToKey.get(sortedValues[i - 1][0] as V1) as string,
              ) || null
            nextIndex =
              i + 1 < sortedValues.length
                ? newIndices.get(
                    valueToKey.get(sortedValues[i + 1][0] as V1) as string,
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

      // Pre-compute valid previous and next indices for each position
      // This avoids repeated lookups during index generation
      const validPrevIndices: (string | null)[] = new Array(sortedValues.length)
      const validNextIndices: (string | null)[] = new Array(sortedValues.length)

      // Initialize with null values
      validPrevIndices.fill(null)
      validNextIndices.fill(null)

      // First element has no previous
      validPrevIndices[0] = null

      // Last element has no next
      validNextIndices[sortedValues.length - 1] = null

      // Compute next valid indices (working forward)
      let lastValidNextIndex: string | null = null
      for (let i = sortedValues.length - 1; i >= 0; i--) {
        const valueKey = valueToKey.get(sortedValues[i][0] as V1) as string

        // Set the next index for the current position
        validNextIndices[i] = lastValidNextIndex

        // Update lastValidNextIndex if this element has an index
        if (newIndices.has(valueKey)) {
          lastValidNextIndex = newIndices.get(valueKey) || null
        } else {
          const existingEntry = prevOutputMap.get(valueKey)
          if (existingEntry) {
            lastValidNextIndex = existingEntry[1]
          }
        }
      }

      // Compute previous valid indices (working backward)
      let lastValidPrevIndex: string | null = null
      for (let i = 0; i < sortedValues.length; i++) {
        const valueKey = valueToKey.get(sortedValues[i][0] as V1) as string

        // Set the previous index for the current position
        validPrevIndices[i] = lastValidPrevIndex

        // Update lastValidPrevIndex if this element has an index
        if (newIndices.has(valueKey)) {
          lastValidPrevIndex = newIndices.get(valueKey) || null
        } else {
          const existingEntry = prevOutputMap.get(valueKey)
          if (existingEntry) {
            lastValidPrevIndex = existingEntry[1]
          }
        }
      }

      // Second pass: assign new indices for values that don't have one or need to be updated
      for (let i = 0; i < sortedValues.length; i++) {
        const [value, _multiplicity] = sortedValues[i]
        // Use the pre-computed valueKey
        const valueKey = valueToKey.get(value as V1) as string

        if (!newIndices.has(valueKey)) {
          // This value doesn't have an index yet, use pre-computed indices
          prevIndex = validPrevIndices[i]
          nextIndex = validNextIndices[i]

          const newIndex = generateKeyBetween(prevIndex, nextIndex)
          newIndices.set(valueKey, newIndex)

          // Update validPrevIndices for subsequent elements
          if (i < sortedValues.length - 1 && validPrevIndices[i + 1] === null) {
            validPrevIndices[i + 1] = newIndex
          }
        }
      }

      // Now create the output with the new indices
      for (let i = 0; i < sortedValues.length; i++) {
        const [value, _multiplicity] = sortedValues[i]
        // Use the pre-computed valueKey
        const valueKey = valueToKey.get(value as V1) as string
        const index = newIndices.get(valueKey)!

        // Check if this is a new value or if the index has changed
        const existingEntry = prevOutputMap.get(valueKey)

        if (!existingEntry) {
          // New value
          result.push([[key, [value as V1, index]], 1])
          this.#indexOut.addValue(key, [[value as V1, index], 1])
        } else if (existingEntry[1] !== index) {
          // Index has changed, remove old entry and add new one
          result.push([[key, existingEntry], -1])
          result.push([[key, [value as V1, index]], 1])
          this.#indexOut.addValue(key, [existingEntry, -1])
          this.#indexOut.addValue(key, [[value as V1, index], 1])
        }
        // If the value exists and the index hasn't changed, do nothing
      }
    }

    if (result.length > 0) {
      this.output.sendData(new MultiSet(result))
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
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
