import { StreamBuilder } from '../../d2.js'
import { IStreamBuilder, KeyValue } from '../../types.js'
import { DifferenceStreamReader, DifferenceStreamWriter } from '../../graph.js'
import { Antichain } from '../../order.js'
import { SQLiteDb } from '../database.js'
import { ReduceOperatorSQLite } from './reduce.js'
import { SQLiteContext } from '../context.js'

export class DistinctOperatorSQLite<K, V> extends ReduceOperatorSQLite<
  K,
  V,
  V
> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, V]>,
    initialFrontier: Antichain,
    db: SQLiteDb,
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

    super(id, inputA, output, distinctInner, initialFrontier, db)
  }
}

/**
 * Removes duplicates by key
 * Persists state to SQLite
 *
 * @param db - Optional SQLite database (can be injected via context)
 */
export function distinct<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V extends T extends KeyValue<K, infer V> ? V : never,
  T,
>(db?: SQLiteDb) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, V>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for distinct operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const output = new StreamBuilder<KeyValue<K, V>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, V>>(),
    )
    const operator = new DistinctOperatorSQLite<K, V>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
      output.writer,
      stream.graph.frontier(),
      database,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
