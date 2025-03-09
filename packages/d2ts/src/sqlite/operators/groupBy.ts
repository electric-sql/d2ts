import { IStreamBuilder, KeyValue } from '../../types.js'
import { map } from '../../operators/map.js'
import { reduce } from './reduce.js'
import { SQLiteDb } from '../database.js'
import { SQLiteContext } from '../context.js'

// Re-export most of the aggregate functions from the main implementation
// since they are compatible with both versions
export { sum, count, avg, min, max, median } from '../../operators/groupBy.js'

// Define types locally since they're not exported from the main implementation
type GroupKey = Record<string, unknown>

type BasicAggregateFunction<T, R, V = unknown> = {
  preMap: (data: T) => V
  reduce: (values: [V, number][]) => V
  postMap?: (result: V) => R
}

type PipedAggregateFunction<T, R> = {
  pipe: (stream: IStreamBuilder<T>) => IStreamBuilder<KeyValue<string, R>>
}

type AggregateFunction<T, R, V = unknown> =
  | BasicAggregateFunction<T, R, V>
  | PipedAggregateFunction<T, R>

type ExtractAggregateReturnType<T, A> =
  A extends AggregateFunction<T, infer R, any> ? R : never

type AggregatesReturnType<T, A> = {
  [K in keyof A]: ExtractAggregateReturnType<T, A[K]>
}

/**
 * Checks if an aggregate function is a piped aggregate function
 */
function isPipedAggregateFunction<T, R>(
  aggregate: AggregateFunction<T, R>,
): aggregate is PipedAggregateFunction<T, R> {
  return 'pipe' in aggregate
}

/**
 * Creates a mode aggregate function that computes the most frequent value in a group
 * SQLite-compatible version that uses a serializable object instead of Map
 * @param valueExtractor Function to extract a value from each data entry
 */
export function mode<T>(
  valueExtractor: (value: T) => number = (v) => v as unknown as number,
): AggregateFunction<T, number, Record<string, number>> {
  return {
    preMap: (data: T) => {
      const value = valueExtractor(data)
      // Use a plain object instead of Map for better serialization
      const frequencyObj: Record<string, number> = {}
      frequencyObj[value.toString()] = 1
      return frequencyObj
    },
    reduce: (values: [Record<string, number>, number][]) => {
      // Combine all frequency objects
      const combinedFrequencies: Record<string, number> = {}

      for (const [freqObj, multiplicity] of values) {
        for (const [valueStr, count] of Object.entries(freqObj)) {
          const currentCount = combinedFrequencies[valueStr] || 0
          combinedFrequencies[valueStr] = currentCount + count * multiplicity
        }
      }

      return combinedFrequencies
    },
    postMap: (result: Record<string, number>) => {
      const entries = Object.entries(result)
      if (entries.length === 0) return 0

      let modeValue = 0
      let maxFrequency = 0

      for (const [valueStr, frequency] of entries) {
        const value = Number(valueStr)
        if (frequency > maxFrequency) {
          maxFrequency = frequency
          modeValue = value
        }
      }

      return modeValue
    },
  }
}

/**
 * Groups data by key and applies multiple aggregate operations
 * SQLite version that persists state to a database
 *
 * @param keyExtractor Function to extract grouping key from data
 * @param aggregates Object mapping aggregate names to aggregate functions
 * @param db Optional SQLite database (can be injected via context)
 */
export function groupBy<
  T,
  K extends GroupKey,
  A extends Record<string, AggregateFunction<T, any, any>>,
>(keyExtractor: (data: T) => K, aggregates: A, db?: SQLiteDb) {
  // Get database from context if not provided explicitly
  const database = db || SQLiteContext.getDb()

  if (!database) {
    throw new Error(
      'SQLite database is required for groupBy operator. ' +
        'Provide it as a parameter or use withSQLite() to inject it.',
    )
  }

  type ResultType = K & AggregatesReturnType<T, A>

  const basicAggregates = Object.fromEntries(
    Object.entries(aggregates).filter(
      ([_, aggregate]) => !isPipedAggregateFunction(aggregate),
    ),
  ) as Record<string, BasicAggregateFunction<T, any, any>>

  // @ts-expect-error - TODO: we don't use this yet, but we will
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const pipedAggregates = Object.fromEntries(
    Object.entries(aggregates).filter(([_, aggregate]) =>
      isPipedAggregateFunction(aggregate),
    ),
  ) as Record<string, PipedAggregateFunction<T, any>>

  return (
    stream: IStreamBuilder<T>,
  ): IStreamBuilder<KeyValue<string, ResultType>> => {
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
        for (const [name, aggregate] of Object.entries(basicAggregates)) {
          values[name] = aggregate.preMap(data)
        }

        return [keyString, values] as KeyValue<string, Record<string, unknown>>
      }),
    )

    // Then reduce to compute aggregates, using the SQLite version of reduce
    const reduced = withKeysAndValues.pipe(
      reduce((values) => {
        const result: Record<string, unknown> = {}

        // Get the original key from first value in group
        const originalKey = values[0][0][KEY_SENTINEL]
        result[KEY_SENTINEL] = originalKey

        // Apply each aggregate function
        for (const [name, aggregate] of Object.entries(basicAggregates)) {
          const preValues = values.map(
            ([v, m]) => [v[name], m] as [any, number],
          )
          result[name] = aggregate.reduce(preValues)
        }

        return [[result, 1]]
      }, database),
    )

    // Finally map to extract the key and include all values
    return reduced.pipe(
      map(([keyString, values]) => {
        // Extract the original key
        const key = values[KEY_SENTINEL] as K

        // Create intermediate result with key values and aggregate results
        const result: Record<string, unknown> = {}

        // Add key properties to result
        Object.assign(result, key)

        // Apply postMap if provided
        for (const [name, aggregate] of Object.entries(basicAggregates)) {
          if (aggregate.postMap) {
            result[name] = aggregate.postMap(values[name])
          } else {
            result[name] = values[name]
          }
        }

        // Return with the string key instead of the object
        return [keyString, result] as KeyValue<string, ResultType>
      }),
    )
  }
}
