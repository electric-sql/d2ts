import {
  filter,
  map,
  keyBy,
  orderBy,
  orderByWithIndex,
  orderByWithFractionalIndex,
  topK,
  topKWithIndex,
  topKWithFractionalIndex,
} from '../operators/index.js'
import {
  groupBy,
  sum,
  count,
  avg,
  min,
  max,
  median,
  mode,
} from '../operators/groupBy.js'
import { IStreamBuilder } from '../types.js'
import { Query, Condition, ConditionOperand, FunctionCall } from './schema.js'
import {
  extractValueFromNestedRow,
  evaluateOperandOnNestedRow,
} from './extractors.js'
import { evaluateConditionOnNestedRow } from './evaluators.js'
import { processJoinClause } from './joins.js'

// Helper function to determine if an object is a function call with an aggregate function
function isAggregateFunctionCall(obj: any): boolean {
  if (!obj || typeof obj !== 'object') return false

  const aggregateFunctions = [
    'SUM',
    'COUNT',
    'AVG',
    'MIN',
    'MAX',
    'MEDIAN',
    'MODE',
  ]
  const keys = Object.keys(obj)

  return keys.length === 1 && aggregateFunctions.includes(keys[0])
}

// Helper function to determine if an object is an ORDER_INDEX function call
function isOrderIndexFunctionCall(obj: any): boolean {
  if (!obj || typeof obj !== 'object') return false

  const keys = Object.keys(obj)
  return keys.length === 1 && keys[0] === 'ORDER_INDEX'
}

// Helper function to extract the ORDER_INDEX type from a function call
function getOrderIndexType(obj: any): 'numeric' | 'fractional' {
  if (!isOrderIndexFunctionCall(obj)) {
    throw new Error('Not an ORDER_INDEX function call')
  }

  const arg = obj['ORDER_INDEX']
  if (arg === 'numeric' || arg === true || arg === 'default') {
    return 'numeric'
  } else if (arg === 'fractional') {
    return 'fractional'
  } else {
    throw new Error('Invalid ORDER_INDEX type: ' + arg)
  }
}

// Helper function to get an aggregate function based on the function name
function getAggregateFunction(
  functionName: string,
  columnRef: string | ConditionOperand,
  mainTableAlias: string,
) {
  // Create a value extractor function for the column to aggregate
  const valueExtractor = (nestedRow: Record<string, unknown>) => {
    let value: unknown
    if (typeof columnRef === 'string' && columnRef.startsWith('@')) {
      value = extractValueFromNestedRow(
        nestedRow,
        columnRef.substring(1),
        mainTableAlias,
      )
    } else {
      value = evaluateOperandOnNestedRow(
        nestedRow,
        columnRef as ConditionOperand,
        mainTableAlias,
      )
    }
    // Ensure we return a number for aggregate functions
    return typeof value === 'number' ? value : 0
  }

  // Return the appropriate aggregate function
  switch (functionName.toUpperCase()) {
    case 'SUM':
      return sum(valueExtractor)
    case 'COUNT':
      return count() // count() doesn't need a value extractor
    case 'AVG':
      return avg(valueExtractor)
    case 'MIN':
      return min(valueExtractor)
    case 'MAX':
      return max(valueExtractor)
    case 'MEDIAN':
      return median(valueExtractor)
    case 'MODE':
      return mode(valueExtractor)
    default:
      throw new Error(`Unsupported aggregate function: ${functionName}`)
  }
}

// Helper function to extract all columns from a table in a nested row
function extractAllColumnsFromTable(
  nestedRow: Record<string, unknown>,
  tableAlias: string,
): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  // Get the table data
  const tableData = nestedRow[tableAlias] as
    | Record<string, unknown>
    | null
    | undefined

  if (!tableData || typeof tableData !== 'object') {
    return result
  }

  // Add all columns from the table to the result
  for (const [columnName, value] of Object.entries(tableData)) {
    result[columnName] = value
  }

  return result
}

// Helper function to extract all columns from all tables in a nested row
function extractAllColumnsFromAllTables(
  nestedRow: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  // Process each table in the nested row
  for (const [tableAlias, tableData] of Object.entries(nestedRow)) {
    if (tableData && typeof tableData === 'object') {
      // Add all columns from this table to the result
      // If there are column name conflicts, the last table's columns will overwrite previous ones
      Object.assign(result, extractAllColumnsFromTable(nestedRow, tableAlias))
    }
  }

  return result
}

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
  // Create a copy of the inputs map to avoid modifying the original
  const allInputs = { ...inputs }

  // Process WITH queries if they exist
  if (query.with && query.with.length > 0) {
    // Process each WITH query in order
    for (const withQuery of query.with) {
      // Ensure the WITH query has an alias
      if (!withQuery.as) {
        throw new Error('WITH query must have an "as" property')
      }

      // Ensure the WITH query is not keyed
      if ((withQuery as Query).keyBy !== undefined) {
        throw new Error('WITH query cannot have a "keyBy" property')
      }

      // Check if this CTE name already exists in the inputs
      if (allInputs[withQuery.as]) {
        throw new Error(`CTE with name "${withQuery.as}" already exists`)
      }

      // Create a new query without the 'with' property to avoid circular references
      const withQueryWithoutWith = { ...withQuery, with: undefined }

      // Compile the WITH query using the current set of inputs
      // (which includes previously compiled WITH queries)
      const compiledWithQuery = compileQuery(
        withQueryWithoutWith,
        allInputs,
      ) as IStreamBuilder<Record<string, unknown>>

      // Add the compiled query to the inputs map using its alias
      allInputs[withQuery.as] = compiledWithQuery
    }
  }

  // Create a map of table aliases to inputs
  const tables: Record<string, IStreamBuilder<Record<string, unknown>>> = {}

  // The main table is the one in the FROM clause
  const mainTableAlias = query.as || query.from

  // Get the main input from the inputs map (now including CTEs)
  const input = allInputs[query.from]
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
    pipeline = processJoinClause(
      pipeline,
      query,
      tables,
      mainTableAlias,
      allInputs,
    )
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

  // Process the GROUP BY clause if it exists
  if (query.groupBy) {
    // Normalize groupBy to an array of column references
    const groupByColumns = Array.isArray(query.groupBy)
      ? query.groupBy
      : [query.groupBy]

    // Create a key extractor function for the groupBy operator
    const keyExtractor = (nestedRow: Record<string, unknown>) => {
      const key: Record<string, unknown> = {}

      // Extract each groupBy column value
      for (const column of groupByColumns) {
        if (typeof column === 'string' && column.startsWith('@')) {
          const columnRef = column.substring(1)
          const columnName = columnRef.includes('.')
            ? columnRef.split('.')[1]
            : columnRef

          key[columnName] = extractValueFromNestedRow(
            nestedRow,
            columnRef,
            mainTableAlias,
          )
        }
      }

      return key
    }

    // Create aggregate functions for any aggregated columns in the SELECT clause
    const aggregates: Record<string, any> = {}

    // Scan the SELECT clause for aggregate functions
    for (const item of query.select) {
      if (typeof item === 'object') {
        for (const [alias, expr] of Object.entries(item)) {
          if (typeof expr === 'object' && isAggregateFunctionCall(expr)) {
            // Get the function name (the only key in the object)
            const functionName = Object.keys(expr)[0]
            // Get the column reference or expression to aggregate
            const columnRef = (expr as FunctionCall)[
              functionName as keyof FunctionCall
            ]

            // Add the aggregate function to our aggregates object
            aggregates[alias] = getAggregateFunction(
              functionName,
              columnRef,
              mainTableAlias,
            )
          }
        }
      }
    }

    // Apply the groupBy operator if we have any aggregates
    if (Object.keys(aggregates).length > 0) {
      pipeline = pipeline.pipe(
        groupBy(keyExtractor, aggregates),
        // Convert KeyValue<string, ResultType> to Record<string, unknown>
        map(([_key, value]) => {
          // After groupBy, the value already contains both the key fields and aggregate results
          // We need to return it as is, not wrapped in a nested structure
          return value as Record<string, unknown>
        }),
      )
    }
  }

  // Process the HAVING clause if it exists
  // This works similarly to WHERE but is applied after any aggregations
  if (query.having) {
    pipeline = pipeline.pipe(
      filter((row) => {
        // For HAVING, we're working with the flattened row that contains both
        // the group by keys and the aggregate results directly
        const result = evaluateConditionOnNestedRow(
          { [mainTableAlias]: row, ...row } as Record<string, unknown>,
          query.having as Condition,
          mainTableAlias,
        )
        return result
      }),
    )
  }

  // Process the SELECT clause - this is where we flatten the structure
  pipeline = pipeline.pipe(
    map((nestedRow: Record<string, unknown>) => {
      const result: Record<string, unknown> = {}

      // Check if this is a grouped result (has no nested table structure)
      // If it's a grouped result, we need to handle it differently
      const isGroupedResult =
        query.groupBy &&
        Object.keys(nestedRow).some(
          (key) =>
            !Object.keys(inputs).includes(key) &&
            typeof nestedRow[key] !== 'object',
        )

      for (const item of query.select) {
        if (typeof item === 'string') {
          // Handle wildcard select - all columns from all tables
          if (item === '@*') {
            // For grouped results, just return the row as is
            if (isGroupedResult) {
              Object.assign(result, nestedRow)
            } else {
              // Extract all columns from all tables
              Object.assign(result, extractAllColumnsFromAllTables(nestedRow))
            }
            continue
          }

          // Handle @table.* syntax - all columns from a specific table
          if (item.startsWith('@') && item.endsWith('.*')) {
            const tableAlias = item.slice(1, -2) // Remove the '@' and '.*' parts

            // For grouped results, check if we have columns from this table
            if (isGroupedResult) {
              // In grouped results, we don't have the nested structure anymore
              // So we can't extract by table. Just continue to the next item.
              continue
            } else {
              // Extract all columns from the specified table
              Object.assign(
                result,
                extractAllColumnsFromTable(nestedRow, tableAlias),
              )
            }
            continue
          }

          // Handle simple column references like "@table.column" or "@column"
          if (item.startsWith('@')) {
            const parts = item.split(' as ')
            const columnRef = parts[0].substring(1)
            const alias = parts.length > 1 ? parts[1].trim() : columnRef

            // For grouped results, check if the column is directly in the row first
            if (isGroupedResult && columnRef in nestedRow) {
              result[alias] = nestedRow[columnRef]
            } else {
              // Extract the value from the nested structure
              result[alias] = extractValueFromNestedRow(
                nestedRow,
                columnRef,
                mainTableAlias,
                undefined,
              )
            }

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

              // For grouped results, check if the column is directly in the row first
              if (isGroupedResult && columnRef in nestedRow) {
                result[alias] = nestedRow[columnRef]
              } else {
                // Extract the value from the nested structure
                result[alias] = extractValueFromNestedRow(
                  nestedRow,
                  columnRef,
                  mainTableAlias,
                  undefined,
                )
              }
            } else if (typeof expr === 'string' && !expr.startsWith('@')) {
              // Handle expressions like "table1.col * table2.col"
              // This would need more advanced parsing - for now just log
              // Future: Parse and evaluate the expression
            } else if (typeof expr === 'object') {
              // For grouped results, the aggregate results are already in the row
              if (isGroupedResult && alias in nestedRow) {
                result[alias] = nestedRow[alias]
              } else {
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
      }

      return result
    }),
  )

  let resultPipeline: IStreamBuilder<
    Record<string, unknown> | [string | number, Record<string, unknown>]
  > = pipeline

  // Process keyBy parameter if it exists
  if (query.keyBy) {
    const keyByParam = query.keyBy // Store in a local variable to avoid undefined issues

    resultPipeline = pipeline.pipe(
      keyBy((row: Record<string, unknown>) => {
        if (Array.isArray(keyByParam)) {
          // Multiple columns - extract values and JSON stringify
          const keyValues: Record<string, unknown> = {}
          for (const keyColumn of keyByParam) {
            // Remove @ prefix if present
            const columnName = keyColumn.startsWith('@')
              ? keyColumn.substring(1)
              : keyColumn

            if (columnName in row) {
              keyValues[columnName] = row[columnName]
            } else {
              throw new Error(
                `Key column "${columnName}" not found in result set. Make sure it's included in the select clause.`,
              )
            }
          }
          return JSON.stringify(keyValues)
        } else {
          // Single column
          // Remove @ prefix if present
          const columnName = keyByParam.startsWith('@')
            ? keyByParam.substring(1)
            : keyByParam

          if (!(columnName in row)) {
            throw new Error(
              `Key column "${columnName}" not found in result set. Make sure it's included in the select clause.`,
            )
          }

          const keyValue = row[columnName]
          // Use the value directly if it's a string or number, otherwise JSON stringify
          if (typeof keyValue === 'string' || typeof keyValue === 'number') {
            return keyValue
          } else {
            return JSON.stringify(keyValue)
          }
        }
      }),
    )
  }

  // Process orderBy parameter if it exists
  if (query.orderBy) {
    // Check if any column in the SELECT clause is an ORDER_INDEX function call
    let hasOrderIndexColumn = false
    let orderIndexType: 'numeric' | 'fractional' = 'numeric'
    let orderIndexAlias = ''

    // Scan the SELECT clause for ORDER_INDEX functions
    for (const item of query.select) {
      if (typeof item === 'object') {
        for (const [alias, expr] of Object.entries(item)) {
          if (typeof expr === 'object' && isOrderIndexFunctionCall(expr)) {
            hasOrderIndexColumn = true
            orderIndexAlias = alias
            orderIndexType = getOrderIndexType(expr)
            break
          }
        }
      }
      if (hasOrderIndexColumn) break
    }

    // Normalize orderBy to an array of objects
    const orderByItems: Array<{
      operand: ConditionOperand
      direction: 'asc' | 'desc'
    }> = []

    if (typeof query.orderBy === 'string') {
      // Handle string format: '@column'
      orderByItems.push({
        operand: query.orderBy,
        direction: 'asc',
      })
    } else if (Array.isArray(query.orderBy)) {
      // Handle array format: ['@column1', { '@column2': 'desc' }]
      for (const item of query.orderBy) {
        if (typeof item === 'string') {
          orderByItems.push({
            operand: item,
            direction: 'asc',
          })
        } else if (typeof item === 'object') {
          for (const [column, direction] of Object.entries(item)) {
            orderByItems.push({
              operand: column,
              direction: direction as 'asc' | 'desc',
            })
          }
        }
      }
    } else if (typeof query.orderBy === 'object') {
      // Handle object format: { '@column': 'desc' }
      for (const [column, direction] of Object.entries(query.orderBy)) {
        orderByItems.push({
          operand: column,
          direction: direction as 'asc' | 'desc',
        })
      }
    }

    // Create a value extractor function for the orderBy operator
    const valueExtractor = (value: unknown) => {
      const row = value as Record<string, unknown>

      // Create a nested row structure for evaluateOperandOnNestedRow
      const nestedRow: Record<string, unknown> = { [mainTableAlias]: row }

      // For multiple orderBy columns, create a composite key
      if (orderByItems.length > 1) {
        return orderByItems.map((item) => {
          const value = evaluateOperandOnNestedRow(
            nestedRow,
            item.operand,
            mainTableAlias,
          )

          // Reverse the value for 'desc' ordering
          return item.direction === 'desc' && typeof value === 'number'
            ? -value
            : item.direction === 'desc' && typeof value === 'string'
              ? String.fromCharCode(
                  ...[...value].map((c) => 0xffff - c.charCodeAt(0)),
                )
              : value
        })
      } else if (orderByItems.length === 1) {
        // For a single orderBy column, use the value directly
        const item = orderByItems[0]
        const value = evaluateOperandOnNestedRow(
          nestedRow,
          item.operand,
          mainTableAlias,
        )

        // Reverse the value for 'desc' ordering
        return item.direction === 'desc' && typeof value === 'number'
          ? -value
          : item.direction === 'desc' && typeof value === 'string'
            ? String.fromCharCode(
                ...[...value].map((c) => 0xffff - c.charCodeAt(0)),
              )
            : value
      }

      // Default case - no ordering
      return null
    }

    const comparator = (a: unknown, b: unknown): number => {
      // if a and b are both numbers compare them directly
      if (typeof a === 'number' && typeof b === 'number') {
        return a - b
      }
      // if a and b are both strings, compare them lexicographically
      if (typeof a === 'string' && typeof b === 'string') {
        return a.localeCompare(b)
      }
      // if a and b are both booleans, compare them
      if (typeof a === 'boolean' && typeof b === 'boolean') {
        return a ? 1 : -1
      }
      // if a and b are both dates, compare them
      if (a instanceof Date && b instanceof Date) {
        return a.getTime() - b.getTime()
      }
      // if a and b are both null, return 0
      if (a === null && b === null) {
        return 0
      }
      // if a and b are both arrays, compare them element by element
      if (Array.isArray(a) && Array.isArray(b)) {
        for (let i = 0; i < a.length; i++) {
          const result = comparator(a[i], b[i])
          if (result !== 0) return result
        }
        return 0
      }
      // if a and b are both null/undefined, return 0
      if ((a === null || a === undefined) && (b === null || b === undefined)) {
        return 0
      }
      // Fallback to string comparison for all other cases
      return (a as any).toString().localeCompare((b as any).toString())
    }

    let topKComparator: (a: unknown, b: unknown) => number
    if (!query.keyBy) {
      topKComparator = (a, b) => {
        const aValue = valueExtractor(a)
        const bValue = valueExtractor(b)
        return comparator(aValue, bValue)
      }
    }

    // Apply the appropriate orderBy operator based on whether an ORDER_INDEX column is requested
    if (hasOrderIndexColumn) {
      if (orderIndexType === 'numeric') {
        if (query.keyBy) {
          // Use orderByWithIndex for numeric indices
          resultPipeline = resultPipeline.pipe(
            orderByWithIndex(valueExtractor, {
              limit: query.limit,
              offset: query.offset,
              comparator,
            }),
            map(([key, [value, index]]) => {
              // Add the index to the result
              const result = {
                ...(value as Record<string, unknown>),
                [orderIndexAlias]: index,
              }
              return [key, result]
            }),
          )
        } else {
          // Use topKWithIndex for numeric indices
          resultPipeline = resultPipeline.pipe(
            map((value) => [null, value]),
            topKWithIndex(topKComparator!, {
              limit: query.limit,
              offset: query.offset,
            }),
            map(([_, [value, index]]) => {
              // Add the index to the result
              return {
                ...(value as Record<string, unknown>),
                [orderIndexAlias]: index,
              }
            }),
          )
        }
      } else {
        if (query.keyBy) {
          // Use orderByWithFractionalIndex for fractional indices
          resultPipeline = resultPipeline.pipe(
            orderByWithFractionalIndex(valueExtractor, {
              limit: query.limit,
              offset: query.offset,
              comparator,
            }),
            map(([key, [value, index]]) => {
              // Add the index to the result
              const result = {
                ...(value as Record<string, unknown>),
                [orderIndexAlias]: index,
              }
              return [key, result]
            }),
          )
        } else {
          // Use topKWithFractionalIndex for fractional indices
          resultPipeline = resultPipeline.pipe(
            map((value) => [null, value]),
            topKWithFractionalIndex(topKComparator!, {
              limit: query.limit,
              offset: query.offset,
            }),
            map(([_, [value, index]]) => {
              // Add the index to the result
              return {
                ...(value as Record<string, unknown>),
                [orderIndexAlias]: index,
              }
            }),
          )
        }
      }
    } else {
      if (query.keyBy) {
        // Use regular orderBy if no index column is requested and but a keyBy is requested
        resultPipeline = resultPipeline.pipe(
          orderBy(valueExtractor, {
            limit: query.limit,
            offset: query.offset,
            comparator,
          }),
        )
      } else {
        // Use topK if no index column is requested and no keyBy is requested
        resultPipeline = resultPipeline.pipe(
          map((value) => [null, value]),
          topK(topKComparator!, {
            limit: query.limit,
            offset: query.offset,
          }),
          map(([_, value]) => value as Record<string, unknown>),
        )
      }
    }
  } else if (query.limit !== undefined || query.offset !== undefined) {
    // If there's a limit or offset without orderBy, throw an error
    throw new Error(
      'LIMIT and OFFSET require an ORDER BY clause to ensure deterministic results',
    )
  }

  return resultPipeline as T
}
