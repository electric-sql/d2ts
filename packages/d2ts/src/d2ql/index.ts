/**
 * D2QL is a SQL-like query language for D2TS.
 * It's a subset of SQL, encoded as JSON/TypeScript, that's compiled to a D2 pipeline.
 */

export * from './schema.js'
export * from './types.js'
export { compileQuery } from './compiler.js'
