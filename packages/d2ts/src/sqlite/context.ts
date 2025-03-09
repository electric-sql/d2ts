import { SQLiteDb } from './database.js'
import { PipedOperator } from '../types.js'

/**
 * Context for storing the SQLite database
 * This is used for dependency injection of SQLite database into operators
 */
class SQLiteContext {
  private static db: SQLiteDb | null = null

  /**
   * Set the database to use for all SQLite operators
   * @param db - SQLite database instance
   */
  static setDb(db: SQLiteDb): void {
    SQLiteContext.db = db
  }

  /**
   * Get the current database
   * @returns SQLite database instance or null
   */
  static getDb(): SQLiteDb | null {
    return SQLiteContext.db
  }

  /**
   * Clear the database reference
   */
  static clear(): void {
    SQLiteContext.db = null
  }
}

/**
 * Provide a SQLite database to a pipeline of operators
 *
 * This function creates a context where the database is available to all
 * SQLite operators in the pipeline without explicitly passing it.
 *
 * @example
 * ```
 * // Create a SQLite database
 * const db = new BetterSQLite3Wrapper(sqlite)
 *
 * // Use with database injection
 * input.pipe(
 *   withSQLite(db)(
 *     map((x) => x + 1),
 *     reduce((vals) => {
 *       let sum = 0
 *       for (const [val, diff] of vals) {
 *         sum += val * diff
 *       }
 *       return [[sum, 1]]
 *     }),
 *     distinct()
 *   )
 * )
 * ```
 *
 * @param db - The SQLite database to use
 * @returns A function that wraps pipeline operators and injects the database
 */
export function withSQLite(db: SQLiteDb) {
  return <T, U>(
    ...operators: PipedOperator<any, any>[]
  ): PipedOperator<T, U> => {
    return (stream) => {
      // Set the database for this pipeline
      const previousDb = SQLiteContext.getDb()
      SQLiteContext.setDb(db)

      try {
        // Apply all operators in sequence
        let result = stream
        for (const op of operators) {
          result = op(result)
        }
        return result as any
      } finally {
        // Restore the previous database (if any)
        if (previousDb) {
          SQLiteContext.setDb(previousDb)
        } else {
          SQLiteContext.clear()
        }
      }
    }
  }
}

export { SQLiteContext }
