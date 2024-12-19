/**
 * Represents a prepared statement in SQLite
 */
export interface SQLiteStatement<Params = unknown, Result = unknown> {
  /**
   * Run the prepared statement with parameters
   * Accepts either an array of parameters or an object of named parameters
   */
  run(...params: Params extends any[] ? Params : [Params?]): void

  /**
   * Get a single row from the prepared statement
   * Accepts either an array of parameters or an object of named parameters
   */
  get(...params: Params extends any[] ? Params : [Params?]): Result | undefined

  /**
   * Get all rows from the prepared statement
   * Accepts either an array of parameters or an object of named parameters
   */
  all(...params: Params extends any[] ? Params : [Params?]): Result[]
}

/**
 * Interface for SQLite database operations
 */
export interface SQLiteDb {
  /**
   * Execute raw SQL
   */
  exec(sql: string): void

  /**
   * Prepare a statement
   */
  prepare<Params = unknown, Result = unknown>(
    sql: string,
  ): SQLiteStatement<Params, Result>
}

/**
 * Wrapper for better-sqlite3 to implement SQLiteDb interface
 */
export class BetterSQLite3Wrapper implements SQLiteDb {
  #db: import('better-sqlite3').Database

  constructor(db: import('better-sqlite3').Database) {
    this.#db = db
  }

  exec(sql: string): void {
    this.#db.exec(sql)
  }

  prepare<Params = unknown, Result = unknown>(
    sql: string,
  ): SQLiteStatement<Params, Result> {
    const stmt = this.#db.prepare(sql)
    return {
      run: (...params: Params extends any[] ? Params : [Params?]) => 
        stmt.run(...params),
      get: (...params: Params extends any[] ? Params : [Params?]) => 
        stmt.get(...params) as Result | undefined,
      all: (...params: Params extends any[] ? Params : [Params?]) => 
        stmt.all(...params) as Result[],
    }
  }

  close(): void {
    this.#db.close()
  }
}
