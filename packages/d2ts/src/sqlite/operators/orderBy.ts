import { IStreamBuilder } from '../../types.js'
import { KeyValue } from '../../types.js'
import { topK, topKWithIndex } from './topK.js'
import { topKWithFractionalIndex } from './topKWithFractionalIndex.js'
import { map } from '../../operators/map.js'
import { innerJoin } from './join.js'
import { consolidate } from './consolidate.js'
import { SQLiteDb } from '../database.js'
import { SQLiteContext } from '../context.js'

interface OrderByOptions<Ve> {
  comparator?: (a: Ve, b: Ve) => number
  limit?: number
  offset?: number
  db?: SQLiteDb
}

/**
 * Orders the elements and limits the number of results, with optional offset
 * This requires a keyed stream, and uses the `topK` operator to order all the elements.
 *
 * @param valueExtractor - A function that extracts the value to order by from the element
 * @param options - An optional object containing comparator, limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function orderBy<T extends KeyValue<unknown, unknown>, Ve = unknown>(
  valueExtractor: (
    value: T extends KeyValue<unknown, infer V> ? V : never,
  ) => Ve,
  options?: OrderByOptions<Ve>,
) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    // Get database from context if not provided explicitly
    const database = options?.db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for orderBy operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const limit = options?.limit ?? Infinity
    const offset = options?.offset ?? 0
    const comparator =
      options?.comparator ??
      ((a: Ve, b: Ve) => {
        // Default to JS like ordering
        if (a === b) return 0
        if (a < b) return -1
        return 1
      })

    type K = T extends KeyValue<infer K, unknown> ? K : never

    return stream.pipe(
      map(
        ([key, value]) =>
          [
            null,
            [
              key,
              valueExtractor(
                value as T extends KeyValue<unknown, infer V> ? V : never,
              ),
            ],
          ] as KeyValue<null, [K, Ve]>,
      ),
      topK((a, b) => comparator(a[1], b[1]), { limit, offset, db: database }),
      map(([_, [key]]) => [key, null] as KeyValue<K, null>),
      innerJoin(stream, database),
      map(([key, value]) => {
        return [key, value[1]] as T
      }),
      consolidate(database),
    )
  }
}

/**
 * Orders the elements and limits the number of results, with optional offset and
 * annotates the value with the index.
 * This requires a keyed stream, and uses the `topKWithIndex` operator to order all the elements.
 *
 * @param valueExtractor - A function that extracts the value to order by from the element
 * @param options - An optional object containing comparator, limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function orderByWithIndex<
  T extends KeyValue<unknown, unknown>,
  Ve = unknown,
>(
  valueExtractor: (
    value: T extends KeyValue<unknown, infer V> ? V : never,
  ) => Ve,
  options?: OrderByOptions<Ve>,
) {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<
    KeyValue<
      T extends KeyValue<infer K, unknown> ? K : never,
      [T extends KeyValue<unknown, infer V> ? V : never, number]
    >
  > => {
    // Get database from context if not provided explicitly
    const database = options?.db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for orderByWithIndex operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const limit = options?.limit ?? Infinity
    const offset = options?.offset ?? 0
    const comparator =
      options?.comparator ??
      ((a: Ve, b: Ve) => {
        // Default to JS like ordering
        if (a === b) return 0
        if (a < b) return -1
        return 1
      })

    type K = T extends KeyValue<infer K, unknown> ? K : never
    type V = T extends KeyValue<unknown, infer V> ? V : never

    return stream.pipe(
      map(
        ([key, value]) =>
          [
            null,
            [
              key,
              valueExtractor(
                value as T extends KeyValue<unknown, infer V> ? V : never,
              ),
            ],
          ] as KeyValue<null, [K, Ve]>,
      ),
      topKWithIndex((a, b) => comparator(a[1], b[1]), {
        limit,
        offset,
        db: database,
      }),
      map(([_, [[key], index]]) => [key, index] as KeyValue<K, number>),
      innerJoin(stream, database),
      map(([key, [index, value]]) => {
        return [key, [value, index]] as KeyValue<K, [V, number]>
      }),
      consolidate(database),
    )
  }
}

/**
 * Orders the elements and limits the number of results, with optional offset and
 * annotates the value with a fractional index.
 * This requires a keyed stream, and uses the `topKWithFractionalIndex` operator to order all the elements.
 *
 * @param valueExtractor - A function that extracts the value to order by from the element
 * @param db - Optional SQLite database (can be injected via context)
 * @param options - An optional object containing comparator, limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function orderByWithFractionalIndex<
  T extends KeyValue<unknown, unknown>,
  Ve = unknown,
>(
  valueExtractor: (
    value: T extends KeyValue<unknown, infer V> ? V : never,
  ) => Ve,
  options?: OrderByOptions<Ve>,
) {
  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<
    KeyValue<
      T extends KeyValue<infer K, unknown> ? K : never,
      [T extends KeyValue<unknown, infer V> ? V : never, string]
    >
  > => {
    // Get database from context if not provided explicitly
    const database = options?.db || SQLiteContext.getDb()

    if (!database) {
      throw new Error(
        'SQLite database is required for orderByWithFractionalIndex operator. ' +
          'Provide it as a parameter or use withSQLite() to inject it.',
      )
    }

    const limit = options?.limit ?? Infinity
    const offset = options?.offset ?? 0
    const comparator =
      options?.comparator ??
      ((a: Ve, b: Ve) => {
        // Default to JS like ordering
        if (a === b) return 0
        if (a < b) return -1
        return 1
      })

    type K = T extends KeyValue<infer K, unknown> ? K : never
    type V = T extends KeyValue<unknown, infer V> ? V : never

    return stream.pipe(
      map(
        ([key, value]) =>
          [
            null,
            [
              key,
              valueExtractor(
                value as T extends KeyValue<unknown, infer V> ? V : never,
              ),
            ],
          ] as KeyValue<null, [K, Ve]>,
      ),
      topKWithFractionalIndex((a, b) => comparator(a[1], b[1]), {
        limit,
        offset,
        db: database,
      }),
      map(([_, [[key], index]]) => [key, index] as KeyValue<K, string>),
      innerJoin(stream, database),
      map(([key, [index, value]]) => {
        return [key, [value, index]] as KeyValue<K, [V, string]>
      }),
      consolidate(database),
    )
  }
}
