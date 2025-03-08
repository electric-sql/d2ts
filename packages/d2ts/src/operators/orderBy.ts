import { IStreamBuilder } from '../types'
import { KeyValue } from '../types.js'
import { topK, indexedTopK } from './topK.js'
import { fractionalIndexedTopK } from './fractionalIndexedTopK.js'
import { map } from './map.js'
import { innerJoin } from './join.js'
import { consolidate } from './consolidate.js'

interface OrderByOptions<Ve> {
  comparator?: (a: Ve, b: Ve) => number
  limit?: number
  offset?: number
}

/**
 * Orders the elements and limits the number of results, with optional offset
 * This requires a keyed stream, and uses the `topK` operator to order all the elements.
 *
 * @param valueExtractor - A function that extracts the value to order by from the element
 * @param options - An optional object containing comparator, limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function orderBy<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  T,
  Ve,
>(valueExtractor: (value: V1) => Ve, options?: OrderByOptions<Ve>) {
  const limit = options?.limit ?? Infinity
  const offset = options?.offset ?? 0
  const comparator =
    options?.comparator ??
    ((a, b) => {
      // Default to JS like ordering
      if (a === b) return 0
      if (a < b) return -1
      return 1
    })

  return (
    stream: IStreamBuilder<KeyValue<K, V1>>,
  ): IStreamBuilder<KeyValue<K, V1>> => {
    return stream.pipe(
      map(
        ([key, value]) =>
          [null, [key, valueExtractor(value)]] as KeyValue<null, [K, Ve]>,
      ),
      topK((a, b) => comparator(a[1], b[1]), { limit, offset }),
      map(([_, [key]]) => [key, null] as KeyValue<K, null>),
      innerJoin(stream),
      map(([key, value]) => {
        return [key, value[1]] as KeyValue<K, V1>
      }),
      consolidate(),
    )
  }
}

/**
 * Orders the elements and limits the number of results, with optional offset and 
 * annotates the value with the index.
 * This requires a keyed stream, and uses the `topK` operator to order all the elements.
 *
 * @param valueExtractor - A function that extracts the value to order by from the element
 * @param options - An optional object containing comparator, limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function orderByWithIndex<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  T,
  Ve,
>(valueExtractor: (value: V1) => Ve, options?: OrderByOptions<Ve>) {
  const limit = options?.limit ?? Infinity
  const offset = options?.offset ?? 0
  const comparator =
    options?.comparator ??
    ((a, b) => {
      // Default to JS like ordering
      if (a === b) return 0
      if (a < b) return -1
      return 1
    })

  return (
    stream: IStreamBuilder<KeyValue<K, V1>>,
  ): IStreamBuilder<KeyValue<K, [V1, number]>> => {
    return stream.pipe(
      map(
        ([key, value]) =>
          [null, [key, valueExtractor(value)]] as KeyValue<null, [K, Ve]>,
      ),
      indexedTopK((a, b) => comparator(a[1], b[1]), { limit, offset }),
      map(([_, [[key], index]]) => [key, index] as KeyValue<K, number>),
      innerJoin(stream),
      map(([key, [index, value]]) => {
        return [key, [value, index]] as KeyValue<K, [V1, number]>
      }),
      consolidate(),
    )
  }
}

/**
 * Orders the elements and limits the number of results, with optional offset and 
 * annotates the value with a fractional index.
 * This requires a keyed stream, and uses the `topK` operator to order all the elements.
 *
 * @param valueExtractor - A function that extracts the value to order by from the element
 * @param options - An optional object containing comparator, limit and offset properties
 * @returns A piped operator that orders the elements and limits the number of results
 */
export function orderByWithFractionalIndex<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  T,
  Ve,
>(valueExtractor: (value: V1) => Ve, options?: OrderByOptions<Ve>) {
  const limit = options?.limit ?? Infinity
  const offset = options?.offset ?? 0
  const comparator =
    options?.comparator ??
    ((a, b) => {
      // Default to JS like ordering
      if (a === b) return 0
      if (a < b) return -1
      return 1
    })

  return (
    stream: IStreamBuilder<KeyValue<K, V1>>,
  ): IStreamBuilder<KeyValue<K, [V1, string]>> => {
    return stream.pipe(
      map(
        ([key, value]) =>
          [null, [key, valueExtractor(value)]] as KeyValue<null, [K, Ve]>,
      ),
      fractionalIndexedTopK((a, b) => comparator(a[1], b[1]), { limit, offset }),
      map(([_, [[key], index]]) => [key, index] as KeyValue<K, string>),
      innerJoin(stream),
      map(([key, [index, value]]) => {
        return [key, [value, index]] as KeyValue<K, [V1, string]>
      }),
      consolidate(),
    )
  }
}
