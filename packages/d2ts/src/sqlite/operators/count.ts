import { StreamBuilder } from '../../d2.js'
import { IStreamBuilder, KeyValue } from '../../types.js'
import { DifferenceStreamReader, DifferenceStreamWriter } from '../../graph.js'
import { Antichain } from '../../order.js'
import { SQLiteDb } from '../database.js'
import { ReduceOperatorSQLite } from './reduce.js'
import { SQLiteContext } from '../context.js'

export class CountOperatorSQLite<K, V> extends ReduceOperatorSQLite<
  K,
  V,
  number
> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, number]>,
    initialFrontier: Antichain,
    db: SQLiteDb,
  ) {
    const countInner = (vals: [V, number][]): [number, number][] => {
      let count = 0
      for (const [_, diff] of vals) {
        count += diff
      }
      return [[count, 1]]
    }

    super(id, inputA, output, countInner, initialFrontier, db)
  }
}

/**
 * Counts the number of elements by key
 * Persists state to SQLite
 *
 * @param db - Optional SQLite database (can be injected via context)
 */
export function count<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V extends T extends KeyValue<K, infer V> ? V : never,
  T,
>(db?: SQLiteDb) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, number>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for count operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const output = new StreamBuilder<KeyValue<K, number>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, number>>(),
    )
    const operator = new CountOperatorSQLite<K, V>(
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
