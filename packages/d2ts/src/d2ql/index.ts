/**
 * D2QL is a SQL-like query language for D2.
 * It's a subset of SQL, encoded as JSON/TypeScript, that's compiled to a D2 pipeline.
 */

// Export the schema types
export * from './schema.js'

// Export the compiler functions
export { compileQuery, createPipeline } from './compiler.js'
