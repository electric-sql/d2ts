import {
  filter,
  map,
  join as joinOperator,
  JoinType,
  consolidate,
} from '../operators/index.js'
import { IStreamBuilder } from '../types.js'
import { Query, Condition, ConditionOperand } from './schema.js'
import {
  extractValueFromNestedRow,
  extractJoinKey,
  evaluateOperandOnNestedRow,
} from './extractors.js'
import { evaluateConditionOnNestedRow } from './evaluators.js'
import { processJoinResults } from './joins.js'

/**
 * Compiles a D2QL query into a D2 pipeline
 * @param query The D2QL query to compile
 * @param inputs Mapping of table names to input streams
 * @returns A stream builder representing the compiled query
 */
export function compileQuery<T extends IStreamBuilder<unknown>>(
  query: Query,
  inputs: Record<string, IStreamBuilder<Record<string, unknown>>>,
): T {
  // Create a map of table aliases to inputs
  const tables: Record<string, IStreamBuilder<Record<string, unknown>>> = {}

  // The main table is the one in the FROM clause
  const mainTableAlias = query.as || query.from

  // Get the main input from the inputs map
  const input = inputs[query.from]
  if (!input) {
    throw new Error(`Input for table "${query.from}" not found in inputs map`)
  }

  tables[mainTableAlias] = input

  // Prepare the initial pipeline with the main table wrapped in its alias
  let pipeline = input.pipe(
    map((row: unknown) => {
      // Initialize the record with a nested structure
      return { [mainTableAlias]: row } as Record<string, unknown>
    }),
  )

  // Process JOIN clauses if they exist
  if (query.join) {
    for (const joinClause of query.join) {
      // Create a stream for the joined table
      const joinedTableAlias = joinClause.as || joinClause.from

      // Get the right join type for the operator
      const joinType: JoinType =
        joinClause.type === 'cross' ? 'inner' : joinClause.type

      // We need to prepare the main pipeline and the joined pipeline
      // to have the correct key format for joining
      const mainPipeline = pipeline.pipe(
        map((nestedRow: Record<string, unknown>) => {
          // Extract the key from the ON condition left side for the main table
          const mainRow = nestedRow[mainTableAlias] as Record<string, unknown>

          // Extract the join key from the main row
          const keyValue = extractJoinKey(
            mainRow,
            joinClause.on[0],
            mainTableAlias,
          )

          // Return [key, nestedRow] as a KeyValue type
          return [keyValue, nestedRow] as [unknown, Record<string, unknown>]
        }),
      )

      // Get the joined table input from the inputs map
      let joinedTableInput: IStreamBuilder<Record<string, unknown>>

      if (inputs[joinClause.from]) {
        // Use the provided input if available
        joinedTableInput = inputs[joinClause.from]
      } else {
        // Create a new input if not provided
        joinedTableInput = input.graph.newInput<Record<string, unknown>>()
      }

      tables[joinedTableAlias] = joinedTableInput

      // Create a pipeline for the joined table
      const joinedPipeline = joinedTableInput.pipe(
        map((row: Record<string, unknown>) => {
          // Wrap the row in an object with the table alias as the key
          const nestedRow = { [joinedTableAlias]: row }

          // Extract the key from the ON condition right side for the joined table
          const keyValue = extractJoinKey(
            row,
            joinClause.on[2],
            joinedTableAlias,
          )

          // Return [key, nestedRow] as a KeyValue type
          return [keyValue, nestedRow] as [unknown, Record<string, unknown>]
        }),
      )

      // Apply join with appropriate typings based on join type
      switch (joinType) {
        case 'inner':
          pipeline = mainPipeline.pipe(
            joinOperator(joinedPipeline, 'inner'),
            consolidate(),
            processJoinResults(mainTableAlias, joinedTableAlias, joinClause),
          )
          break
        case 'left':
          pipeline = mainPipeline.pipe(
            joinOperator(joinedPipeline, 'left'),
            consolidate(),
            processJoinResults(mainTableAlias, joinedTableAlias, joinClause),
          )
          break
        case 'right':
          pipeline = mainPipeline.pipe(
            joinOperator(joinedPipeline, 'right'),
            consolidate(),
            processJoinResults(mainTableAlias, joinedTableAlias, joinClause),
          )
          break
        case 'full':
          pipeline = mainPipeline.pipe(
            joinOperator(joinedPipeline, 'full'),
            consolidate(),
            processJoinResults(mainTableAlias, joinedTableAlias, joinClause),
          )
          break
        default:
          pipeline = mainPipeline.pipe(
            joinOperator(joinedPipeline, 'inner'),
            consolidate(),
            processJoinResults(mainTableAlias, joinedTableAlias, joinClause),
          )
      }
    }
  }

  // Process the WHERE clause if it exists
  if (query.where) {
    pipeline = pipeline.pipe(
      filter((nestedRow) => {
        const result = evaluateConditionOnNestedRow(
          nestedRow as Record<string, unknown>,
          query.where as Condition,
          mainTableAlias,
        )
        return result
      }),
    )
  }

  // Note: In the future, GROUP BY would be implemented here

  // Process the HAVING clause if it exists
  // This works similarly to WHERE but is applied after any aggregations
  if (query.having) {
    pipeline = pipeline.pipe(
      filter((nestedRow) => {
        const result = evaluateConditionOnNestedRow(
          nestedRow as Record<string, unknown>,
          query.having as Condition,
          mainTableAlias,
        )
        return result
      }),
    )
  }

  // Process the SELECT clause - this is where we flatten the structure
  const resultPipeline = pipeline.pipe(
    map((nestedRow: Record<string, unknown>) => {
      const result: Record<string, unknown> = {}

      for (const item of query.select) {
        if (typeof item === 'string') {
          // Handle simple column references like "@table.column" or "@column"
          if (item.startsWith('@')) {
            const parts = item.split(' as ')
            const columnRef = parts[0].substring(1)
            const alias = parts.length > 1 ? parts[1].trim() : columnRef

            // Extract the value from the nested structure
            result[alias] = extractValueFromNestedRow(
              nestedRow,
              columnRef,
              mainTableAlias,
              undefined,
            )

            // If the alias contains a dot (table.column) and there's no explicit 'as',
            // use just the column part as the field name
            if (alias.includes('.') && parts.length === 1) {
              const columnName = alias.split('.')[1]
              result[columnName] = result[alias]
              delete result[alias]
            }
          }
        } else {
          // Handle aliased columns like { alias: "@column_name" }
          for (const [alias, expr] of Object.entries(item)) {
            if (typeof expr === 'string' && expr.startsWith('@')) {
              const columnRef = expr.substring(1)
              // Extract the value from the nested structure
              result[alias] = extractValueFromNestedRow(
                nestedRow,
                columnRef,
                mainTableAlias,
                undefined,
              )
            } else if (typeof expr === 'string' && !expr.startsWith('@')) {
              // Handle expressions like "table1.col * table2.col"
              // This would need more advanced parsing - for now just log
              // Future: Parse and evaluate the expression
            } else if (typeof expr === 'object') {
              // This might be a function call
              result[alias] = evaluateOperandOnNestedRow(
                nestedRow,
                expr as ConditionOperand,
                mainTableAlias,
                undefined,
              )
            }
          }
        }
      }

      return result
    }),
  )

  return resultPipeline as T
}
