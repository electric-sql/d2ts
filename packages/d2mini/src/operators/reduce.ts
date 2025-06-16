import { IStreamBuilder, DataMessage, MessageType, KeyValue } from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { MultiSet } from '../multiset.js'
import { Antichain, Version } from '../order.js'
import { Index } from '../version-index.js'

/**
 * Base operator for reduction operations
 */
export class ReduceOperator<K, V1, V2> extends UnaryOperator<[K, V1 | V2]> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, V2>()
  #keysTodo = new Map<Version, Set<K>>()
  #f: (values: [V1, number][]) => [V2, number][]

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, V2]>,
    f: (values: [V1, number][]) => [V2, number][],
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#f = f
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
      const result: [[K, V2], number][] = []

      for (const key of keys) {
        const curr = this.#index.reconstructAt(key, version)
        const currOut = this.#indexOut.reconstructAt(key, version)
        const out = this.#f(curr)

        // Calculate delta between current and previous output
        const delta = new Map<string, number>()
        const values = new Map<string, V2>()
        for (const [value, multiplicity] of out) {
          const valueKey = JSON.stringify(value)
          values.set(valueKey, value)
          delta.set(valueKey, (delta.get(valueKey) || 0) + multiplicity)
        }
        for (const [value, multiplicity] of currOut) {
          const valueKey = JSON.stringify(value)
          values.set(valueKey, value)
          delta.set(valueKey, (delta.get(valueKey) || 0) - multiplicity)
        }

        // Add non-zero deltas to result
        for (const [valueKey, multiplicity] of delta) {
          const value = values.get(valueKey)!
          if (multiplicity !== 0) {
            result.push([[key, value], multiplicity])
            this.#indexOut.addValue(key, version, [value, multiplicity])
          }
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
 * Reduces the elements in the stream by key
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
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
