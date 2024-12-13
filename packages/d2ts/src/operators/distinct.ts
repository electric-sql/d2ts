import { IStreamBuilder, KeyValue } from '../types.js'
import { DifferenceStreamReader, DifferenceStreamWriter } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { ReduceOperator } from './reduce.js'
import { Antichain } from '../order.js'

/**
 * Operator that removes duplicates by key
 */
export class DistinctOperator<K, V> extends ReduceOperator<K, V, V> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, V]>,
    initialFrontier: Antichain,
  ) {
    const distinctInner = (vals: [V, number][]): [V, number][] => {
      const consolidated = new Map<string, number>()
      const values = new Map<string, V>()
      for (const [val, diff] of vals) {
        const key = JSON.stringify(val)
        consolidated.set(key, (consolidated.get(key) || 0) + diff)
        values.set(key, val)
      }
      return Array.from(consolidated.entries())
        .filter(([_, count]) => count > 0)
        .map(([key, _]) => [values.get(key) as V, 1])
    }

    super(id, inputA, output, distinctInner, initialFrontier)
  }
}

/**
 * Removes duplicates by key
 */
export function distinct<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V extends T extends KeyValue<K, infer V> ? V : never,
  T,
>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, V>> => {
    const output = new StreamBuilder<KeyValue<K, V>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, V>>(),
    )
    const operator = new DistinctOperator<K, V>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
} 