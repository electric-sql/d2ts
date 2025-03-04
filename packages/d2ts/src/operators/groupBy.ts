import { IStreamBuilder, KeyValue } from '../types.js'
import { map } from './map.js'
import { reduce } from './reduce.js'

type GroupKey = Record<string, unknown>
type AggregateFunction<T, R, V = unknown> = {
  preMap: (data: T) => V
  reduce: (values: [V, number][]) => V
  postMap?: (result: V) => R
}

/**
 * Groups data by key and applies multiple aggregate operations
 * @param keyExtractor Function to extract grouping key from data
 * @param aggregates Object mapping aggregate names to aggregate functions
 * @param postMap Optional function to transform the final result
 */
export function groupBy<T, K extends GroupKey, R = Record<string, unknown>>(
  keyExtractor: (data: T) => K,
  aggregates: Record<string, AggregateFunction<T, any, any>>,
) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<string, R>> => {
    // Special key to store the original key object
    const KEY_SENTINEL = '__original_key__'

    // First map to extract keys and pre-aggregate values
    const withKeysAndValues = stream.pipe(
      map((data) => {
        const key = keyExtractor(data)
        const keyString = JSON.stringify(key)

        // Create values object with pre-aggregated values
        const values: Record<string, unknown> = {}

        // Store the original key object
        values[KEY_SENTINEL] = key

        // Add pre-aggregated values
        for (const [name, aggregate] of Object.entries(aggregates)) {
          values[name] = aggregate.preMap(data)
        }

        return [keyString, values] as KeyValue<string, Record<string, unknown>>
      }),
    )

    // Then reduce to compute aggregates
    const reduced = withKeysAndValues.pipe(
      reduce((values) => {
        const result: Record<string, unknown> = {}

        // Get the original key from first value in group
        const originalKey = values[0][0][KEY_SENTINEL]
        result[KEY_SENTINEL] = originalKey

        // Apply each aggregate function
        for (const [name, aggregate] of Object.entries(aggregates)) {
          const preValues = values.map(
            ([v, m]) => [v[name], m] as [any, number],
          )
          result[name] = aggregate.reduce(preValues)
        }

        return [[result, 1]]
      }),
    )

    // Finally map to extract the key and include all values
    return reduced.pipe(
      map(([keyString, values]) => {
        // Extract the original key
        const key = values[KEY_SENTINEL] as K

        // Create intermediate result with key values and aggregate results
        const result: Record<string, unknown> = {}
        delete result[KEY_SENTINEL]

        // Add key properties to result
        Object.assign(result, key)

        // Apply postMap if provided
        for (const [name, aggregate] of Object.entries(aggregates)) {
          if (aggregate.postMap) {
            result[name] = aggregate.postMap(values[name])
          } else {
            result[name] = values[name]
          }
        }

        // Return with the string key instead of the object
        return [keyString, result] as KeyValue<string, R>
      }),
    )
  }
}

/**
 * Creates a sum aggregate function
 */
export function sum<T>(
  valueExtractor: (value: T) => number = (v) => v as unknown as number,
): AggregateFunction<T, number, number> {
  return {
    preMap: (data: T) => valueExtractor(data),
    reduce: (values: [number, number][]) => {
      let total = 0
      for (const [value, multiplicity] of values) {
        total += value * multiplicity
      }
      return total
    },
  }
}

/**
 * Creates a count aggregate function
 */
export function count<T>(): AggregateFunction<T, number, number> {
  return {
    preMap: () => 1,
    reduce: (values: [number, number][]) => {
      let count = 0
      for (const [_, multiplicity] of values) {
        count += multiplicity
      }
      return count
    },
  }
}

/**
 * Creates an average aggregate function
 */
export function avg<T>(
  valueExtractor: (value: T) => number = (v) => v as unknown as number,
): AggregateFunction<T, number, { sum: number; count: number }> {
  return {
    preMap: (data: T) => ({
      sum: valueExtractor(data),
      count: 0,
    }),
    reduce: (values: [{ sum: number; count: number }, number][]) => {
      let totalSum = 0
      let totalCount = 0
      for (const [value, multiplicity] of values) {
        totalSum += value.sum * multiplicity
        totalCount += multiplicity
      }
      return {
        sum: totalSum,
        count: totalCount,
      }
    },
    postMap: (result: { sum: number; count: number }) => {
      return result.sum / result.count
    },
  }
}

/**
 * Creates a min aggregate function that computes the minimum value in a group
 * @param valueExtractor Function to extract a numeric value from each data entry
 */
export function min<T>(
  valueExtractor: (value: T) => number = (v) => v as unknown as number,
): AggregateFunction<T, number, number> {
  return {
    preMap: (data: T) => valueExtractor(data),
    reduce: (values: [number, number][]) => {
      let minValue = Number.POSITIVE_INFINITY
      for (const [value, _multiplicity] of values) {
        if (value < minValue) {
          minValue = value
        }
      }
      return minValue === Number.POSITIVE_INFINITY ? 0 : minValue
    },
  }
}

/**
 * Creates a max aggregate function that computes the maximum value in a group
 * @param valueExtractor Function to extract a numeric value from each data entry
 */
export function max<T>(
  valueExtractor: (value: T) => number = (v) => v as unknown as number,
): AggregateFunction<T, number, number> {
  return {
    preMap: (data: T) => valueExtractor(data),
    reduce: (values: [number, number][]) => {
      let maxValue = Number.NEGATIVE_INFINITY
      for (const [value, _multiplicity] of values) {
        if (value > maxValue) {
          maxValue = value
        }
      }
      return maxValue === Number.NEGATIVE_INFINITY ? 0 : maxValue
    },
  }
}
