import {
  filter,
  map,
  join as joinOperator,
  JoinType,
  consolidate,
} from '../operators/index.js'
import { groupBy, sum, count, avg, min, max, median, mode } from '../operators/groupBy.js'
import { IStreamBuilder, KeyValue } from '../types.js'
import { Query, Condition, ConditionOperand, FunctionCall } from './schema.js'
import {
  extractValueFromNestedRow,
  extractJoinKey,
  evaluateOperandOnNestedRow,
} from './extractors.js'
import { evaluateConditionOnNestedRow } from './evaluators.js'
import { processJoinResults } from './joins.js'

// Helper function to determine if an object is a function call with an aggregate function
function isAggregateFunctionCall(obj: any): boolean {
  if (!obj || typeof obj !== 'object') return false
  
  const aggregateFunctions = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'MEDIAN', 'MODE'];
  const keys = Object.keys(obj);
  
  return keys.length === 1 && aggregateFunctions.includes(keys[0]);
}

// Helper function to get the appropriate aggregate function
function getAggregateFunction(functionName: string, columnRef: string | ConditionOperand, mainTableAlias: string) {
  // Convert the function name to lowercase to match our operator functions
  const fnName = functionName.toLowerCase();
  
  // Create a value extractor function that will extract the value from the nested row
  const valueExtractor = (nestedRow: Record<string, unknown>) => {
    if (typeof columnRef === 'string' && columnRef.startsWith('@')) {
      const colRef = columnRef.substring(1);
      return extractValueFromNestedRow(nestedRow, colRef, mainTableAlias) as number;
    } else {
      return evaluateOperandOnNestedRow(nestedRow, columnRef as ConditionOperand, mainTableAlias) as number;
    }
  };
  
  // Return the appropriate aggregate function
  switch (fnName) {
    case 'sum':
      return sum(valueExtractor);
    case 'count':
      return count();
    case 'avg':
      return avg(valueExtractor);
    case 'min':
      return min(valueExtractor);
    case 'max':
      return max(valueExtractor);
    case 'median':
      return median(valueExtractor);
    case 'mode':
      return mode(valueExtractor);
    default:
      throw new Error(`Unsupported aggregate function: ${functionName}`);
  }
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

  // Process the GROUP BY clause if it exists
  if (query.groupBy) {
    // Normalize groupBy to an array of column references
    const groupByColumns = Array.isArray(query.groupBy) 
      ? query.groupBy 
      : [query.groupBy];
    
    // Create a key extractor function for the groupBy operator
    const keyExtractor = (nestedRow: Record<string, unknown>) => {
      const key: Record<string, unknown> = {};
      
      // Extract each groupBy column value
      for (const column of groupByColumns) {
        if (typeof column === 'string' && column.startsWith('@')) {
          const columnRef = column.substring(1);
          const columnName = columnRef.includes('.') 
            ? columnRef.split('.')[1] 
            : columnRef;
          
          key[columnName] = extractValueFromNestedRow(
            nestedRow,
            columnRef,
            mainTableAlias
          );
        }
      }
      
      return key;
    };
    
    // Create aggregate functions for any aggregated columns in the SELECT clause
    const aggregates: Record<string, any> = {};
    
    // Scan the SELECT clause for aggregate functions
    for (const item of query.select) {
      if (typeof item === 'object') {
        for (const [alias, expr] of Object.entries(item)) {
          if (typeof expr === 'object' && isAggregateFunctionCall(expr)) {
            // Get the function name (the only key in the object)
            const functionName = Object.keys(expr)[0];
            // Get the column reference or expression to aggregate
            const columnRef = (expr as FunctionCall)[functionName as keyof FunctionCall];
            
            // Add the aggregate function to our aggregates object
            aggregates[alias] = getAggregateFunction(functionName, columnRef, mainTableAlias);
          }
        }
      }
    }
    
    // Apply the groupBy operator if we have any aggregates
    if (Object.keys(aggregates).length > 0) {
      pipeline = pipeline.pipe(
        groupBy(keyExtractor, aggregates),
        // Convert KeyValue<string, ResultType> to Record<string, unknown>
        map(([_key, value]) => value as Record<string, unknown>)
      );
    }
  }

  // Process the HAVING clause if it exists
  // This works similarly to WHERE but is applied after any aggregations
  if (query.having) {
    pipeline = pipeline.pipe(
      filter((nestedRow) => {
        try {
          const result = evaluateConditionOnNestedRow(
            nestedRow as Record<string, unknown>,
            query.having as Condition,
            mainTableAlias,
          )
          return result
        } catch (error) {
          // If there's an error evaluating the condition, log it and filter out the row
          console.error('Error evaluating HAVING condition:', error)
          return false
        }
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
