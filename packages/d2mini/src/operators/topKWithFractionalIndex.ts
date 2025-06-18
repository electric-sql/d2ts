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
import { binarySearch, hash } from '../utils.js'

interface TopKWithFractionalIndexOptions {
  limit?: number
  offset?: number
}

type FractionalIndex = string
type IndexedValue<V> = [V, FractionalIndex]
const indexedValue = <V>(value: V, index: FractionalIndex): IndexedValue<V> => [value, index]
const getValue = <V>(indexedValue: IndexedValue<V>): V => indexedValue[0]
const getIndex = <V>(indexedValue: IndexedValue<V>): FractionalIndex => indexedValue[1]

/**
 * Operator for fractional indexed topK operations
 * This operator maintains fractional indices for sorted elements
 * and only updates indices when elements move position
 */
export class TopKWithFractionalIndexOperator<K, V1> extends UnaryOperator<
  [K, V1],
  [K, IndexedValue<V1>]
> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, IndexedValue<V1>>()
  #comparator: (a: V1, b: V1) => number
  #limit: number
  #offset: number

  /** A map of keys to a sorted array of values for those keys */
  #sortedValues: Map<K, Array<IndexedValue<V1>>> = new Map()

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
    for (const message of this.inputMessages()) {
      for (const [item, multiplicity] of message.getInner()) {
        const [key, value] = item
        this.processElement(key, value, multiplicity)
      }
    }
  }

  processElement(key: K, value: V1, multiplicity: number): void {
    const oldMultiplicity = this.#index.getMultiplicity(key, value)
    this.#index.addValue(key, [value, multiplicity])
    const newMultiplicity = this.#index.getMultiplicity(key, value)

    if (oldMultiplicity <= 0 && newMultiplicity > 0) {
      // The value was invisible but should now be visible
      // Need to insert it into the array of sorted values
      const index = this.insert(key, value)

      // Now check if the top K changed
      const topKStart = this.#offset
      const topKEnd = this.#offset + this.#limit

      if (index < topKEnd) {
        // The inserted element is either before the top K or within the top K
        // If it is before the top K then it moves the the element that was right before the topK into the topK
        // If it is within the top K then the inserted element moves into the top K
        // In both cases the last element of the old top K now moves out of the top K
        const moveInIndex = Math.max(index, topKStart)
        const sortedValues = this.#sortedValues.get(key) ?? []
        if (moveInIndex < sortedValues.length) {
          // We actually have a topK
          // because in some cases there may not be enough elements in the array to reach the start of the topK
          // e.g. [1, 2, 3] with K = 2 and offset = 3 does not have a topK
          const moveInValue = sortedValues[moveInIndex]
          // signal the move in with a +1 diff in the output index
          this.#indexOut.addValue(key, [moveInValue, 1])

          // We need to remove the element that falls out of the top K
          // The element that falls out of the top K has shifted one to the right
          // because of the element we inserted, so we find it at index topKEnd
          if (topKEnd < sortedValues.length) {
            const valueOut = sortedValues[topKEnd]
            this.#indexOut.addValue(key, [valueOut, -1])
          }
        }
      }

    } else if (oldMultiplicity > 0 && newMultiplicity <= 0) {
      // The value was visible but should now be invisible
      // Need to remove it from the array of sorted values
      const [removedElem, removedIndex] = this.remove(key, value)

      // Now check if the top K changed
      const topKStart = this.#offset
      const topKEnd = this.#offset + this.#limit

      if (removedIndex < topKEnd) {
        // The removed element is either before the top K or within the top K
        // If it is before the top K then the first element of the topK moves out of the topK
        // If it is within the top K then the removed element moves out of the topK
        let moveOutValue: IndexedValue<V1> | null = removedElem
        const sortedValues = this.#sortedValues.get(key) ?? []
        if (removedIndex < topKStart) {
          // The removed element is before the topK
          // so actually, the first element of the topK moves out of the topK
          // and not the element that we removed
          // The first element of the topK is now and topKStart - 1
          // since we removed an element before the topK
          const moveOutIndex = topKStart - 1
          if (moveOutIndex < sortedValues.length) {
            moveOutValue = sortedValues[moveOutIndex]
          } else {
            // No value is moving out of the topK
            // because there are no elements in the topK
            moveOutValue = null
          }
        }
        
        if (moveOutValue) {
          // Signal the move out with a -1 diff in the output index
          this.#indexOut.addValue(key, [moveOutValue, -1])
        }

        // Since we removed an element that was before or in the topK
        // the first element after the topK moved one position to the left
        // and hence now falls into the topK
        const moveInIndex = topKEnd - 1
        if (moveInIndex < sortedValues.length) {
          const moveInValue = sortedValues[moveInIndex]
          this.#indexOut.addValue(key, [moveInValue, 1])
        }
      }
    } else {
      // The value was invisible and it remains invisible
      // or it was visible and remains visible
      // so it doesn't affect the topK
      return
    }
  }

  // TODO: see if there is a way to refactor the code for insertions and removals in the topK above
  //       because they are very similar, one is shifting the topK window to the left and the other is shifting it to the right
  //       so i have the feeling there is a common pattern here and we can implement both cases
  //       on top of that pattern

  /**
   * Inserts a value at the correct position
   * into the sorted array of values for the given key.
   * Returns the index of the newly inserted value.
   */
  insert(key: K, value: V1): number {
    // Lookup insert position
    const sortedValues = this.#sortedValues.get(key) ?? []
    const index = binarySearch(sortedValues, indexedValue(value, ''), (a, b) =>
      this.#comparator(getValue(a), getValue(b)),
    )

    // Generate fractional index based on the fractional indices of the elements before and after it
    const indexBefore = index === 0 ? null : getIndex(sortedValues[index - 1])
    const indexAfter =
      index === sortedValues.length ? null : getIndex(sortedValues[index])
    const fractionalIndex = generateKeyBetween(indexBefore, indexAfter)

    // Insert the value at the correct position
    const val = indexedValue(value, fractionalIndex)
    sortedValues.splice(index, 0, val) // O(n) ...
    this.#sortedValues.set(key, sortedValues)
    return index
  }

  /**
   * Removes a value from the sorted array of values for the given key.
   * Returns the index of the removed value.
   * IMPORTANT: this assumes that the value is present in the array
   *            if it's not the case it will remove the element
   *            that is on the position where the provided `value` would be.
   */
  remove(key: K, value: V1): [IndexedValue<V1>, number] {
    const sortedValues = this.#sortedValues.get(key) ?? []
    const index = binarySearch(sortedValues, indexedValue(value, ''), (a, b) =>
      this.#comparator(getValue(a), getValue(b)),
    )
    const [removedElement] = sortedValues.splice(index, 1)
    this.#sortedValues.set(key, sortedValues)
    return [removedElement, index]
  }

  runn(): void {
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
      const curr = this.#index.get(key)
      const currOut = this.#indexOut.get(key)

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
