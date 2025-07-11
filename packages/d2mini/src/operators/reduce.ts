import { IStreamBuilder, KeyValue } from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { LazyMultiSet } from '../multiset.js'
import { Index } from '../indexes.js'
import { hash } from '../utils.js'

/**
 * Base operator for reduction operations (version-free)
 */
export class ReduceOperator<K, V1, V2> extends UnaryOperator<[K, V1], [K, V2]> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, V2>()
  #f: (values: [V1, number][]) => [V2, number][]

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, V2]>,
    f: (values: [V1, number][]) => [V2, number][],
  ) {
    super(id, inputA, output)
    this.#f = f
  }

  run(): void {
    // Collect all input messages and update the index
    const keysTodo = new Set<K>()
    for (const message of this.inputMessages()) {
      for (const [item, multiplicity] of message) {
        const [key, value] = item
        this.#index.addValue(key, [value, multiplicity])
        keysTodo.add(key)
      }
    }

    // Pre-compute all changes and state updates for each key
    const allChanges = new Map<K, { 
      newOutputMap: Map<string, { value: V2; multiplicity: number }>, 
      oldOutputMap: Map<string, { value: V2; multiplicity: number }>,
      commonKeys: Set<string>
    }>()

    // First pass: compute all the output maps for each key
    for (const key of keysTodo) {
      const curr = this.#index.get(key)
      const currOut = this.#indexOut.get(key)
      const out = this.#f(curr)

      // Create maps for current and previous outputs
      const newOutputMap = new Map<
        string,
        { value: V2; multiplicity: number }
      >()
      const oldOutputMap = new Map<
        string,
        { value: V2; multiplicity: number }
      >()

      // Process new output
      for (const [value, multiplicity] of out) {
        const valueKey = hash(value)
        if (newOutputMap.has(valueKey)) {
          newOutputMap.get(valueKey)!.multiplicity += multiplicity
        } else {
          newOutputMap.set(valueKey, { value, multiplicity })
        }
      }

      // Process previous output
      for (const [value, multiplicity] of currOut) {
        const valueKey = hash(value)
        if (oldOutputMap.has(valueKey)) {
          oldOutputMap.get(valueKey)!.multiplicity += multiplicity
        } else {
          oldOutputMap.set(valueKey, { value, multiplicity })
        }
      }

      const commonKeys = new Set<string>()

      // Identify common keys between old and new outputs
      for (const [valueKey] of oldOutputMap) {
        if (newOutputMap.has(valueKey)) {
          commonKeys.add(valueKey)
        }
      }
      for (const [valueKey] of newOutputMap) {
        if (oldOutputMap.has(valueKey)) {
          commonKeys.add(valueKey)
        }
      }

      allChanges.set(key, { newOutputMap, oldOutputMap, commonKeys })
    }

    // Second pass: apply all state updates
    for (const [key, { newOutputMap, oldOutputMap, commonKeys }] of allChanges) {
      // Apply removals to state
      for (const [valueKey, { value, multiplicity }] of oldOutputMap) {
        const newEntry = newOutputMap.get(valueKey)
        if (!newEntry) {
          this.#indexOut.addValue(key, [value, -multiplicity])
        }
      }

      // Apply additions to state
      for (const [valueKey, { value, multiplicity }] of newOutputMap) {
        const oldEntry = oldOutputMap.get(valueKey)
        if (!oldEntry) {
          if (multiplicity !== 0) {
            this.#indexOut.addValue(key, [value, multiplicity])
          }
        }
      }

      // Apply multiplicity changes to state
      for (const valueKey of commonKeys) {
        const newEntry = newOutputMap.get(valueKey)
        const oldEntry = oldOutputMap.get(valueKey)
        const delta = newEntry!.multiplicity - oldEntry!.multiplicity
        if (delta !== 0) {
          this.#indexOut.addValue(key, [newEntry!.value, delta])
        }
      }
    }

    // Create lazy generator that yields results without intermediate array
    const lazyResults = new LazyMultiSet(function* (): Generator<[[K, V2], number], void, unknown> {
      for (const [key, { newOutputMap, oldOutputMap, commonKeys }] of allChanges) {
        // Yield removals for old values that are no longer present
        for (const [valueKey, { value, multiplicity }] of oldOutputMap) {
          const newEntry = newOutputMap.get(valueKey)
          if (!newEntry) {
            yield [[key, value], -multiplicity] as [[K, V2], number]
          }
        }

        // Yield additions for new values that are not present in old
        for (const [valueKey, { value, multiplicity }] of newOutputMap) {
          const oldEntry = oldOutputMap.get(valueKey)
          if (!oldEntry) {
            if (multiplicity !== 0) {
              yield [[key, value], multiplicity] as [[K, V2], number]
            }
          }
        }

        // Yield multiplicity changes for values that were present and are still present
        for (const valueKey of commonKeys) {
          const newEntry = newOutputMap.get(valueKey)
          const oldEntry = oldOutputMap.get(valueKey)
          const delta = newEntry!.multiplicity - oldEntry!.multiplicity
          if (delta !== 0) {
            yield [[key, newEntry!.value], delta] as [[K, V2], number]
          }
        }
      }
    })

    this.output.sendData(lazyResults)
  }
}

/**
 * Reduces the elements in the stream by key (version-free)
 */
export function reduce<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  R,
  T,
>(f: (values: [V1, number][]) => [R, number][]) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, R>> => {
    const output = new StreamBuilder<KeyValue<K, R>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, R>>(),
    )
    const operator = new ReduceOperator<K, V1, R>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      output.writer,
      f,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
