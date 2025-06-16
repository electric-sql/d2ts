import { IStreamBuilder, KeyValue } from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { MultiSet } from '../multiset.js'
import { Index } from '../indexes.js'
import { hash } from '../utils.js'

/**
 * Base operator for reduction operations (version-free)
 */
export class ReduceOperator<K, V1, V2> extends UnaryOperator<[K, V1], [K, V2]> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, V2>()
  #keysTodo = new Set<K>()
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
    for (const message of this.inputMessages()) {
      for (const [item, multiplicity] of message.getInner()) {
        const [key, value] = item
        this.#index.addValue(key, [value, multiplicity])
        this.#keysTodo.add(key)
      }
    }

    // For each key, compute the reduction and delta
    const result: [[K, V2], number][] = []
    for (const key of this.#keysTodo) {
      const curr = this.#index.reconstruct(key)
      const currOut = this.#indexOut.reconstruct(key)
      const out = this.#f(curr)

      // Calculate delta between current and previous output
      const delta = new Map<string | number, number>()
      const values = new Map<string | number, V2>()
      for (const [value, multiplicity] of out) {
        const valueKey = hash(value)
        values.set(valueKey, value)
        delta.set(valueKey, (delta.get(valueKey) || 0) + multiplicity)
      }
      for (const [value, multiplicity] of currOut) {
        const valueKey = hash(value)
        values.set(valueKey, value)
        delta.set(valueKey, (delta.get(valueKey) || 0) - multiplicity)
      }

      // Add non-zero deltas to result
      for (const [valueKey, multiplicity] of delta) {
        const value = values.get(valueKey)!
        if (multiplicity !== 0) {
          result.push([[key, value], multiplicity])
          this.#indexOut.addValue(key, [value, multiplicity])
        }
      }
    }

    if (result.length > 0) {
      this.output.sendData(new MultiSet(result))
    }
    this.#keysTodo.clear()
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
