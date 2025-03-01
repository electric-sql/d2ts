import { D2 } from '../d2.js'
import { filter, map } from '../operators/index.js'
import { IStreamBuilder } from '../types.js'
import {
  Query,
  Condition,
  SimpleCondition,
  Comparator,
  ConditionOperand,
  LogicalOperator,
} from './schema.js'
import { RootStreamBuilder } from '../d2.js'

/**
 * Compiles a D2QL query into a D2 pipeline
 * @param input The input stream to use as the data source
 * @param query The D2QL query to compile
 * @returns A stream builder representing the compiled query
 */
export function compileQuery<T extends Record<string, any>>(
  input: IStreamBuilder<T>,
  query: Query,
): IStreamBuilder<Record<string, any>> {
  // Start building the pipeline
  let pipeline: IStreamBuilder<T> = input

  // Get the main table alias if provided, otherwise use the 'from' value
  const mainTableAlias = query.as || query.from

  // Process the FROM clause - this is just the input source
  // In a more complex implementation, we would look up the input by name

  // Process the WHERE clause if it exists
  if (query.where) {
    pipeline = pipeline.pipe(
      filter((row: T) =>
        evaluateCondition(row, query.where as Condition, mainTableAlias),
      ),
    )
  }

  // Note: In the future, GROUP BY would be implemented here

  // Process the HAVING clause if it exists
  // This works similarly to WHERE but is applied after any aggregations
  if (query.having) {
    pipeline = pipeline.pipe(
      filter((row: T) =>
        evaluateCondition(row, query.having as Condition, mainTableAlias),
      ),
    )
  }

  // Process the SELECT clause
  const resultPipeline = pipeline.pipe(
    map((row: T) => {
      // Create a new object with only the selected columns
      const result: Record<string, any> = {}

      for (const item of query.select) {
        if (typeof item === 'string') {
          // Handle simple column references like "@column_name"
          if (item.startsWith('@')) {
            const columnRef = item.substring(1)
            // Check if it's a table.column reference or just a column reference
            if (columnRef.includes('.')) {
              const [tableAlias, columnName] = columnRef.split('.')
              result[columnName] = row[columnName]
            } else {
              result[columnRef] = row[columnRef]
            }
          }
        } else {
          // Handle aliased columns like { alias: "@column_name" }
          for (const [alias, expr] of Object.entries(item)) {
            if (typeof expr === 'string' && expr.startsWith('@')) {
              const columnRef = expr.substring(1)
              // Check if it's a table.column reference or just a column reference
              if (columnRef.includes('.')) {
                const [tableAlias, columnName] = columnRef.split('.')
                result[alias] = row[columnName]
              } else {
                result[alias] = row[columnRef]
              }
            }
            // We'll handle function calls later
          }
        }
      }

      return result
    }),
  )

  return resultPipeline
}

/**
 * Creates a new D2 pipeline from a D2QL query
 * @param graph The D2 graph to use
 * @param query The D2QL query to compile
 * @returns A tuple containing the input stream and the output stream
 */
export function createPipeline<T extends Record<string, any>>(
  graph: D2,
  query: Query,
): [RootStreamBuilder<T>, IStreamBuilder<Record<string, any>>] {
  const input = graph.newInput<T>()
  const output = compileQuery(input, query)
  return [input, output]
}

/**
 * Evaluates a condition against a row of data
 * @param row The data row to evaluate against
 * @param condition The condition to evaluate
 * @param defaultTableAlias The default table alias to use for column references without an explicit table
 * @returns True if the condition is satisfied, false otherwise
 */
function evaluateCondition<T extends Record<string, any>>(
  row: T,
  condition: Condition,
  defaultTableAlias?: string,
): boolean {
  // For debugging
  // console.log('Evaluating condition:', JSON.stringify(condition));

  // Handle simple conditions with exactly 3 elements
  if (condition.length === 3 && !Array.isArray(condition[0])) {
    const [left, comparator, right] = condition as SimpleCondition
    return evaluateSimpleCondition(
      row,
      left,
      comparator,
      right,
      defaultTableAlias,
    )
  }

  // Handle flat composite conditions (multiple conditions in a single array)
  if (
    condition.length > 3 &&
    !Array.isArray(condition[0]) &&
    typeof condition[1] === 'string' &&
    !['and', 'or'].includes(condition[1] as string)
  ) {
    // Start with the first condition (first 3 elements)
    let result = evaluateSimpleCondition(
      row,
      condition[0],
      condition[1] as Comparator,
      condition[2],
      defaultTableAlias,
    )

    // Process the rest in groups: logical operator, then 3 elements for each condition
    for (let i = 3; i < condition.length; i += 4) {
      const logicalOp = condition[i] as LogicalOperator

      // Make sure we have a complete condition to evaluate
      if (i + 3 <= condition.length) {
        const nextResult = evaluateSimpleCondition(
          row,
          condition[i + 1],
          condition[i + 2] as Comparator,
          condition[i + 3],
          defaultTableAlias,
        )

        // Apply the logical operator
        if (logicalOp === 'and') {
          result = result && nextResult
        } else if (logicalOp === 'or') {
          result = result || nextResult
        }
      }
    }

    return result
  }

  // Handle nested composite conditions where the first element is an array
  if (condition.length > 0 && Array.isArray(condition[0])) {
    // console.log('Evaluating nested condition:', JSON.stringify(condition));

    // Start with the first condition
    let result = evaluateCondition(
      row,
      condition[0] as Condition,
      defaultTableAlias,
    )

    // Process the rest of the conditions and logical operators in pairs
    for (let i = 1; i < condition.length; i += 2) {
      if (i + 1 >= condition.length) break // Make sure we have a pair

      const operator = condition[i] as LogicalOperator
      const nextCondition = condition[i + 1] as Condition

      // Apply the logical operator
      if (operator === 'and') {
        result =
          result && evaluateCondition(row, nextCondition, defaultTableAlias)
      } else if (operator === 'or') {
        result =
          result || evaluateCondition(row, nextCondition, defaultTableAlias)
      }
    }

    return result
  }

  // Fallback - this should not happen with valid conditions
  console.warn('Unsupported condition format:', condition)
  return true
}

/**
 * Evaluates a simple condition against a row of data
 */
function evaluateSimpleCondition<T extends Record<string, any>>(
  row: T,
  left: ConditionOperand,
  comparator: Comparator,
  right: ConditionOperand,
  defaultTableAlias?: string,
): boolean {
  const leftValue = evaluateOperand(row, left, defaultTableAlias)
  const rightValue = evaluateOperand(row, right, defaultTableAlias)

  switch (comparator) {
    case '=':
      return leftValue === rightValue
    case '!=':
      return leftValue !== rightValue
    case '<':
      return leftValue < rightValue
    case '<=':
      return leftValue <= rightValue
    case '>':
      return leftValue > rightValue
    case '>=':
      return leftValue >= rightValue
    case 'like':
      if (typeof leftValue === 'string' && typeof rightValue === 'string') {
        // Simple implementation of LIKE - replace % with .* for regex
        const pattern = rightValue.replace(/%/g, '.*')
        return new RegExp(`^${pattern}$`).test(leftValue)
      }
      return false
    case 'in':
      if (Array.isArray(rightValue)) {
        return rightValue.includes(leftValue)
      }
      return false
    case 'is':
      return leftValue === rightValue
    case 'is not':
      // Properly handle null/undefined checks
      if (rightValue === null) {
        return leftValue !== null && leftValue !== undefined
      }
      return leftValue !== rightValue
    default:
      return false
  }
}

/**
 * Evaluates an operand against a row of data
 * @param row The data row to evaluate against
 * @param operand The operand to evaluate
 * @param defaultTableAlias The default table alias to use for column references without an explicit table
 * @returns The evaluated operand value
 */
function evaluateOperand<T extends Record<string, any>>(
  row: T,
  operand: ConditionOperand,
  defaultTableAlias?: string,
): any {
  // Handle column references
  if (typeof operand === 'string' && operand.startsWith('@')) {
    const columnRef = operand.substring(1)

    // Check if it's a table.column reference
    if (columnRef.includes('.')) {
      const [tableAlias, columnName] = columnRef.split('.')
      // In a more complex implementation with actual joins, we would look up
      // the correct table data based on the alias.
      // For now, we just use the column name directly.
      return row[columnName]
    } else {
      // Simple column reference, use directly
      return row[columnRef]
    }
  }

  // Handle explicit column references
  if (operand && typeof operand === 'object' && 'col' in operand) {
    const colRef = operand.col

    // Check if it's a table.column reference
    if (typeof colRef === 'string' && colRef.includes('.')) {
      const [tableAlias, columnName] = colRef.split('.')
      return row[columnName]
    }

    return row[colRef]
  }

  // Handle explicit literals
  if (operand && typeof operand === 'object' && 'value' in operand) {
    return operand.value
  }

  // Handle literal values
  return operand
}
