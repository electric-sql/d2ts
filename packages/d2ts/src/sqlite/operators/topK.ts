import { IStreamBuilder, PipedOperator } from '../../types.js'
import { KeyValue } from '../../types.js'
import { reduce } from './reduce.js'
import { MultiSet } from '../../multiset.js'
import { SQLiteDb } from '../database.js'
import { SQLiteContext } from '../context.js'

interface TopKOptions {
  limit?: number
  offset?: number
  db?: SQLiteDb
}

/**
 * Limits the number of results based on a comparator, with optional offset.
 * This works on a keyed stream, where the key is the first element of the tuple
 * The ordering is withing a key group, i.e. elements are sorted within a key group
 * and the limit + offset is applied to that sorted group.
 * To order the entire stream, key by the same value for all elements such as null.
 *
 * @param comparator - A function that compares two elements
 * @param db - Optional SQLite database (can be injected via context)
 * @param options - An optional object containing limit and offset properties
 * @returns A piped operator that limits the number of results
 */
export function topK<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  T,
>(
  comparator: (a: V1, b: V1) => number,
  options?: TopKOptions,
): PipedOperator<T, T> {
  const limit = options?.limit ?? Infinity
  const offset = options?.offset ?? 0

  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    // Get database from context if not provided explicitly
    const database = options?.db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for topK operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const reduced = stream.pipe(
      reduce<K, V1, V1, T>((values) => {
        // `values` is a list of tuples, first element is the value, second is the multiplicity
        const consolidated = new MultiSet(values).consolidate()
        const sortedValues = consolidated
          .getInner()
          .sort((a, b) => comparator(a[0] as V1, b[0] as V1))
        return sortedValues.slice(offset, offset + limit)
      }, database),
    )
    return reduced as IStreamBuilder<T>
  }
}

/**
 * Limits the number of results based on a comparator, with optional offset.
 * This works on a keyed stream, where the key is the first element of the tuple
 * The ordering is withing a key group, i.e. elements are sorted within a key group
 * and the limit + offset is applied to that sorted group.
 * To order the entire stream, key by the same value for all elements such as null.
 * Adds the index of the element to the result as [key, [value, index]]
 *
 * @param comparator - A function that compares two elements
 * @param db - Optional SQLite database (can be injected via context)
 * @param options - An optional object containing limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function topKWithIndex<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  T,
>(
  comparator: (a: V1, b: V1) => number,
  options?: TopKOptions,
): PipedOperator<T, KeyValue<K, [V1, number]>> {
  const limit = options?.limit ?? Infinity
  const offset = options?.offset ?? 0

  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<K, [V1, number]>> => {
    // Get database from context if not provided explicitly
    const database = options?.db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for topKWithIndex operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const reduced = stream.pipe(
      reduce<K, V1, [V1, number], T>((values) => {
        // `values` is a list of tuples, first element is the value, second is the multiplicity
        const consolidated = new MultiSet(values).consolidate()
        let i = offset
        const sortedValues = consolidated
          .getInner()
          .sort((a, b) => comparator(a[0] as V1, b[0] as V1))
          .slice(offset, offset + limit)
          .map(([value, multiplicity]): [[V1, number], number] => [
            [value, i++],
            multiplicity,
          ])
        return sortedValues
      }, database),
    )
    return reduced
  }
}
