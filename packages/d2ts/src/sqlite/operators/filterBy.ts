import { IStreamBuilder, PipedOperator } from '../../types.js'
import { KeyValue } from '../../types.js'
import { map } from '../../operators/map.js'
import { innerJoin } from './join.js'
import { consolidate } from './consolidate.js'
import { SQLiteDb } from '../database.js'
import { SQLiteContext } from '../context.js'

/**
 * Filters the elements of a keyed stream, by keys of another stream.
 * This allows you to build pipelies where you have multiple outputs that are related,
 * such as a streams of issues and comments for a project.
 *
 * @param other - The other stream to filter by, which must have the same key type as the input stream
 * @param db - Optional SQLite database (can be injected via context)
 */
export function filterBy<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  T,
>(
  other: IStreamBuilder<KeyValue<K, unknown>>,
  db?: SQLiteDb,
): PipedOperator<T, KeyValue<K, V1>> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, V1>> => {
    // Get database from context if not provided explicitly
    const database = db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for filterBy operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const otherKeys = other.pipe(
      map(([key, _]) => [key, null] as KeyValue<K, null>),
      consolidate(database),
    )
    return stream.pipe(
      innerJoin(otherKeys, database),
      map(([key, [value, _]]) => [key, value] as KeyValue<K, V1>),
    )
  }
}
