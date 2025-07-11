import { IStreamBuilder, PipedOperator } from '../types'
import { KeyValue } from '../types.js'
import { reduce } from './reduce.js'
import { LazyMultiSet } from '../multiset.js'

interface TopKOptions {
  limit?: number
  offset?: number
}

/**
 * Limits the number of results based on a comparator, with optional offset.
 * This works on a keyed stream, where the key is the first element of the tuple
 * The ordering is withing a key group, i.e. elements are sorted within a key group
 * and the limit + offset is applied to that sorted group.
 * To order the entire stream, key by the same value for all elements such as null.
 *
 * @param comparator - A function that compares two elements
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
    const reduced = stream.pipe(
      reduce((values) => {
        // `values` is a list of tuples, first element is the value, second is the multiplicity
        const consolidated = LazyMultiSet.fromArray(values).consolidate()
        const sortedValues = Array.from(consolidated)
          .sort((a, b) => comparator(a[0] as V1, b[0] as V1))
        return sortedValues.slice(offset, offset + limit)
      }),
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
    const reduced = stream.pipe(
      reduce<K, V1, [V1, number], T>((values) => {
        // `values` is a list of tuples, first element is the value, second is the multiplicity
        const consolidated = LazyMultiSet.fromArray(values).consolidate()
        let i = offset
        const sortedValues = Array.from(consolidated)
          .sort((a, b) => comparator(a[0] as V1, b[0] as V1))
          .slice(offset, offset + limit)
          .map(([value, multiplicity]): [[V1, number], number] => [
            [value, i++],
            multiplicity,
          ])
        return sortedValues
      }),
    )
    return reduced
  }
}
