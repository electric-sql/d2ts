import { D2 } from '../d2.js'
import { filter, map, join as joinOperator } from '../operators/index.js'
import { IStreamBuilder } from '../types.js'
import {
  Query,
  Condition,
  SimpleCondition,
  Comparator,
  ConditionOperand,
  LogicalOperator,
  JoinClause,
} from './schema.js'
import { RootStreamBuilder } from '../d2.js'

// Define the SelectItem type based on the Query interface
type SelectItem = string | { [alias: string]: string | any };

/**
 * Compiles a D2QL query into a D2 pipeline
 * @param input The input stream to use as the data source
 * @param query The D2QL query to compile
 * @param joinedInputs Optional map of table names to input streams for joins
 * @returns A stream builder representing the compiled query
 */
export function compileQuery<T extends Record<string, any>>(
  input: IStreamBuilder<T>,
  query: Query,
  joinedInputs?: Record<string, IStreamBuilder<Record<string, any>>>,
): IStreamBuilder<Record<string, any>> {
  // Create a map of table aliases to inputs
  const tables: Record<string, IStreamBuilder<Record<string, any>>> = {}
  
  // The main table is the one in the FROM clause
  const mainTableAlias = query.as || query.from
  tables[mainTableAlias] = input as IStreamBuilder<Record<string, any>>
  
  // Prepare the initial pipeline with the main table wrapped in its alias
  let pipeline = input.pipe(
    map((row: T) => {
      // Initialize the record with a nested structure
      return { [mainTableAlias]: row } as Record<string, any>
    })
  )
  
  // Process JOIN clauses if they exist
  if (query.join) {
    for (const joinClause of query.join) {
      // Create a stream for the joined table
      const joinedTableAlias = joinClause.as || joinClause.from
      
      // Get the right join type for the operator
      const joinType = joinClause.type === 'cross' ? 'inner' : joinClause.type

      // We need to prepare the main pipeline and the joined pipeline
      // to have the correct key format for joining
      const mainPipeline = pipeline.pipe(
        map((nestedRow: Record<string, any>) => {
          // Extract the key from the ON condition left side for the main table
          const mainRow = nestedRow[mainTableAlias];
          
          console.log(`Extracting join key for main table from:`, JSON.stringify(nestedRow));
          console.log(`Using operand:`, JSON.stringify(joinClause.on[0]));
          
          // Extract the join key from the main row
          const keyValue = extractJoinKey(mainRow, joinClause.on[0], mainTableAlias);
          
          console.log(`Main pipeline returning key:`, keyValue, `with row:`, JSON.stringify(nestedRow));
          // Return [key, nestedRow] as a KeyValue type
          return [keyValue, nestedRow] as [any, any];
        })
      );

      // Create a new input for the joined table data
      // This will be populated with data when the query is executed
      let joinedTableInput: IStreamBuilder<Record<string, any>>;
      
      if (joinedInputs && joinedInputs[joinClause.from]) {
        // Use the provided input if available
        console.log(`Using provided input for ${joinClause.from}`);
        joinedTableInput = joinedInputs[joinClause.from];
      } else {
        // Create a new input if not provided
        console.log(`Creating new input for ${joinClause.from}`);
        joinedTableInput = input.graph.newInput<Record<string, any>>();
      }
      
      tables[joinedTableAlias] = joinedTableInput;

      // Create a pipeline for the joined table
      const joinedPipeline = joinedTableInput.pipe(
        map((row: Record<string, any>) => {
          // Wrap the row in an object with the table alias as the key
          const nestedRow = { [joinedTableAlias]: row };
          
          console.log(`Extracting join key for joined table from:`, JSON.stringify(nestedRow));
          console.log(`Using operand:`, JSON.stringify(joinClause.on[2]));
          
          // Extract the key from the ON condition right side for the joined table
          const keyValue = extractJoinKey(row, joinClause.on[2], joinedTableAlias);
          
          console.log(`Joined pipeline returning key:`, keyValue, `with row:`, JSON.stringify(nestedRow));
          // Return [key, nestedRow] as a KeyValue type
          return [keyValue, nestedRow] as [any, any];
        })
      );

      // Now we can join the two pipelines
      pipeline = mainPipeline.pipe(
        // Use a proper type assertion after the join operator
        joinOperator(joinedPipeline, joinType as any),
        // Process the join result and handle nulls in the same step
        map((result: any) => {
          const [key, [mainNestedRow, joinedNestedRow]] = result;
          console.log(`Join result for key ${key}:`, { 
            mainNestedRow: mainNestedRow ? JSON.stringify(mainNestedRow) : 'null', 
            joinedNestedRow: joinedNestedRow ? JSON.stringify(joinedNestedRow) : 'null',
            joinType
          });
          
          // For inner joins, both sides should be non-null
          if (joinType === 'inner') {
            if (!mainNestedRow || !joinedNestedRow) {
              console.log(`Skipping inner join result because of null values`);
              return undefined; // Will be filtered out
            }
          }
          
          // For left joins, the main row must be non-null
          if (joinType === 'left' && !mainNestedRow) {
            console.log(`Skipping left join result because main row is null`);
            return undefined; // Will be filtered out
          }
          
          // For right joins, the joined row must be non-null
          if (joinType === 'right' && !joinedNestedRow) {
            console.log(`Skipping right join result because joined row is null`);
            return undefined; // Will be filtered out
          }
          
          // Merge the nested rows
          const mergedNestedRow: Record<string, any> = {};
          
          // Add main row data if it exists
          if (mainNestedRow) {
            Object.entries(mainNestedRow).forEach(([tableAlias, tableData]) => {
              mergedNestedRow[tableAlias] = tableData;
            });
          }
          
          // If we have a joined row, add it to the merged result
          if (joinedNestedRow) {
            Object.entries(joinedNestedRow).forEach(([tableAlias, tableData]) => {
              mergedNestedRow[tableAlias] = tableData;
            });
          } else if (joinType === 'left' || joinType === 'full') {
            // For left or full joins, add the joined table with null data if missing
            mergedNestedRow[joinedTableAlias] = null;
          }
          
          // For right or full joins, add the main table with null data if missing
          if (!mainNestedRow && (joinType === 'right' || joinType === 'full')) {
            mergedNestedRow[mainTableAlias] = null;
          }
          
          console.log(`Merged row for join:`, JSON.stringify(mergedNestedRow));
          return mergedNestedRow;
        }),
        // Filter out undefined results
        filter((value: unknown): value is Record<string, any> => value !== undefined),
        // Process the ON condition
        filter((nestedRow: Record<string, any>) => {
          // If there's no ON condition, or it's a cross join, always return true
          if (!joinClause.on || joinClause.type === 'cross') {
            return true;
          }
          
          // For LEFT JOIN, if the right side is null, we should include the row
          if (joinClause.type === 'left' && nestedRow[joinedTableAlias] === null) {
            console.log(`LEFT JOIN with null right side, including row regardless of ON condition`);
            return true;
          }
          
          // For RIGHT JOIN, if the left side is null, we should include the row
          if (joinClause.type === 'right' && nestedRow[mainTableAlias] === null) {
            console.log(`RIGHT JOIN with null left side, including row regardless of ON condition`);
            return true;
          }
          
          // For FULL JOIN, if either side is null, we should include the row
          if (joinClause.type === 'full' && (nestedRow[mainTableAlias] === null || nestedRow[joinedTableAlias] === null)) {
            console.log(`FULL JOIN with null side, including row regardless of ON condition`);
            return true;
          }
          
          console.log(`Evaluating ON condition for:`, JSON.stringify(nestedRow));
          const result = evaluateConditionOnNestedRow(
            nestedRow,
            joinClause.on,
            mainTableAlias,
            joinedTableAlias
          );
          console.log(`ON condition result:`, result);
          return result;
        }),
        // Process the WHERE clause for the join if it exists
        filter((nestedRow: Record<string, any>) => {
          if (!joinClause.where) {
            return true;
          }
          
          console.log(`Evaluating WHERE condition for:`, JSON.stringify(nestedRow));
          const result = evaluateConditionOnNestedRow(
            nestedRow,
            joinClause.where,
            mainTableAlias,
            joinedTableAlias
          );
          console.log(`WHERE condition result:`, result);
          return result;
        })
      );
    }
  }

  // Process the WHERE clause if it exists
  if (query.where) {
    pipeline = pipeline.pipe(
      filter((nestedRow) => {
        console.log(`Evaluating main WHERE condition for:`, JSON.stringify(nestedRow));
        const result = evaluateConditionOnNestedRow(nestedRow, query.where as Condition, mainTableAlias);
        console.log(`Main WHERE condition result:`, result);
        return result;
      })
    )
  }

  // Note: In the future, GROUP BY would be implemented here

  // Process the HAVING clause if it exists
  // This works similarly to WHERE but is applied after any aggregations
  if (query.having) {
    pipeline = pipeline.pipe(
      filter((nestedRow) => {
        console.log(`Evaluating HAVING condition for:`, JSON.stringify(nestedRow));
        const result = evaluateConditionOnNestedRow(nestedRow, query.having as Condition, mainTableAlias);
        console.log(`HAVING condition result:`, result);
        return result;
      })
    )
  }

  // Process the SELECT clause - this is where we flatten the structure
  const resultPipeline = pipeline.pipe(
    map((nestedRow: Record<string, any>) => {
      console.log(`Processing SELECT for row:`, JSON.stringify(nestedRow));
      const result: Record<string, any> = {};

      for (const item of query.select) {
        if (typeof item === 'string') {
          // Handle simple column references like "@table.column" or "@column"
          if (item.startsWith('@')) {
            const parts = item.split(' as ');
            const columnRef = parts[0].substring(1);
            const alias = parts.length > 1 ? parts[1].trim() : columnRef;
            
            // Extract the value from the nested structure
            result[alias] = extractValueFromNestedRow(nestedRow, columnRef);
            
            // If the alias contains a dot (table.column) and there's no explicit 'as',
            // use just the column part as the field name
            if (alias.includes('.') && parts.length === 1) {
              const columnName = alias.split('.')[1];
              result[columnName] = result[alias];
              delete result[alias];
            }
          }
        } else {
          // Handle aliased columns like { alias: "@column_name" }
          for (const [alias, expr] of Object.entries(item)) {
            if (typeof expr === 'string' && expr.startsWith('@')) {
              const columnRef = expr.substring(1);
              // Extract the value from the nested structure
              result[alias] = extractValueFromNestedRow(nestedRow, columnRef);
            } else if (typeof expr === 'string' && !expr.startsWith('@')) {
              // Handle expressions like "table1.col * table2.col"
              // This would need more advanced parsing - for now just log
              console.log(`Expression needs evaluation: ${expr}`);
              // Future: Parse and evaluate the expression
            }
          }
        }
      }

      console.log(`SELECT result:`, JSON.stringify(result));
      return result;
    }),
  )

  return resultPipeline
}

/**
 * Extracts a value from a nested row structure
 * @param nestedRow The nested row structure
 * @param columnRef The column reference (may include table.column format)
 * @returns The extracted value or undefined if not found
 */
function extractValueFromNestedRow(
  nestedRow: Record<string, any>,
  columnRef: string
): any {
  console.log(`Extracting value from nested row:`, JSON.stringify(nestedRow));
  console.log(`For column reference:`, columnRef);
  
  // Check if it's a table.column reference
  if (columnRef.includes('.')) {
    const [tableAlias, colName] = columnRef.split('.');
    
    // Get the table data
    const tableData = nestedRow[tableAlias];
    
    if (!tableData) {
      console.log(`Table ${tableAlias} not found in nested row`);
      return null;
    }
    
    // Return the column value from that table
    const value = tableData[colName];
    console.log(`Found value for ${tableAlias}.${colName}:`, value);
    return value;
  } else {
    // If no table is specified, try to find the column in any table
    console.log(`Searching for column ${columnRef} in all tables`);
    for (const [tableAlias, tableData] of Object.entries(nestedRow)) {
      if (tableData && typeof tableData === 'object' && columnRef in tableData) {
        console.log(`Found column ${columnRef} in table ${tableAlias}:`, tableData[columnRef]);
        return tableData[columnRef];
      }
    }
    console.log(`Column ${columnRef} not found in any table`);
    return undefined;
  }
}

/**
 * Evaluates a condition against a nested row structure
 */
function evaluateConditionOnNestedRow(
  nestedRow: Record<string, any>,
  condition: Condition,
  mainTableAlias?: string,
  joinedTableAlias?: string
): boolean {
  // Handle simple conditions with exactly 3 elements
  if (condition.length === 3 && !Array.isArray(condition[0])) {
    const [left, comparator, right] = condition as SimpleCondition
    return evaluateSimpleConditionOnNestedRow(
      nestedRow,
      left,
      comparator as Comparator,
      right,
      mainTableAlias,
      joinedTableAlias
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
    let result = evaluateSimpleConditionOnNestedRow(
      nestedRow,
      condition[0],
      condition[1] as Comparator,
      condition[2],
      mainTableAlias,
      joinedTableAlias
    )

    // Process the rest in groups: logical operator, then 3 elements for each condition
    for (let i = 3; i < condition.length; i += 4) {
      const logicalOp = condition[i] as LogicalOperator

      // Make sure we have a complete condition to evaluate
      if (i + 3 <= condition.length) {
        const nextResult = evaluateSimpleConditionOnNestedRow(
          nestedRow,
          condition[i + 1],
          condition[i + 2] as Comparator,
          condition[i + 3],
          mainTableAlias,
          joinedTableAlias
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
    // Start with the first condition
    let result = evaluateConditionOnNestedRow(
      nestedRow,
      condition[0] as Condition,
      mainTableAlias,
      joinedTableAlias
    )

    // Process the rest of the conditions and logical operators in pairs
    for (let i = 1; i < condition.length; i += 2) {
      if (i + 1 >= condition.length) break // Make sure we have a pair

      const operator = condition[i] as LogicalOperator
      const nextCondition = condition[i + 1] as Condition

      // Apply the logical operator
      if (operator === 'and') {
        result =
          result && evaluateConditionOnNestedRow(
            nestedRow,
            nextCondition,
            mainTableAlias,
            joinedTableAlias
          )
      } else if (operator === 'or') {
        result =
          result || evaluateConditionOnNestedRow(
            nestedRow,
            nextCondition,
            mainTableAlias,
            joinedTableAlias
          )
      }
    }

    return result
  }

  // Fallback - this should not happen with valid conditions
  console.warn('Unsupported condition format:', condition)
  return true
}

/**
 * Evaluates a simple condition against a nested row structure
 */
function evaluateSimpleConditionOnNestedRow(
  nestedRow: Record<string, any>,
  left: ConditionOperand,
  comparator: Comparator,
  right: ConditionOperand,
  mainTableAlias?: string,
  joinedTableAlias?: string
): boolean {
  const leftValue = evaluateOperandOnNestedRow(
    nestedRow, 
    left, 
    mainTableAlias, 
    joinedTableAlias
  )
  
  const rightValue = evaluateOperandOnNestedRow(
    nestedRow, 
    right, 
    mainTableAlias, 
    joinedTableAlias
  )

  // The rest of the function remains the same as evaluateSimpleCondition
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
    case 'not like':
      if (typeof leftValue === 'string' && typeof rightValue === 'string') {
        // Convert SQL LIKE pattern to proper regex pattern
        let pattern = convertLikeToRegex(rightValue)
        const matches = new RegExp(`^${pattern}$`, 'i').test(leftValue)
        return comparator === 'like' ? matches : !matches
      }
      return comparator === 'like' ? false : true
    case 'in':
      // If right value is not an array, we can't do an IN operation
      if (!Array.isArray(rightValue)) {
        return false
      }

      // For empty arrays, nothing is contained in them
      if (rightValue.length === 0) {
        return false
      }

      // Handle array-to-array comparison (check if any element in leftValue exists in rightValue)
      if (Array.isArray(leftValue)) {
        return leftValue.some((item) => isValueInArray(item, rightValue))
      }

      // Handle single value comparison
      return isValueInArray(leftValue, rightValue)

    case 'not in':
      // If right value is not an array, everything is "not in" it
      if (!Array.isArray(rightValue)) {
        return true
      }

      // For empty arrays, everything is "not in" them
      if (rightValue.length === 0) {
        return true
      }

      // Handle array-to-array comparison (check if no element in leftValue exists in rightValue)
      if (Array.isArray(leftValue)) {
        return !leftValue.some((item) => isValueInArray(item, rightValue))
      }

      // Handle single value comparison
      return !isValueInArray(leftValue, rightValue)

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
 * Evaluates an operand against a nested row structure
 */
function evaluateOperandOnNestedRow(
  nestedRow: Record<string, any>,
  operand: ConditionOperand,
  mainTableAlias?: string,
  joinedTableAlias?: string
): any {
  // Handle column references
  if (typeof operand === 'string' && operand.startsWith('@')) {
    const columnRef = operand.substring(1)
    return extractValueFromNestedRow(nestedRow, columnRef)
  }

  // Handle explicit column references
  if (operand && typeof operand === 'object' && 'col' in operand) {
    const colRef = operand.col
    
    if (typeof colRef === 'string') {
      return extractValueFromNestedRow(nestedRow, colRef)
    }
    
    return undefined
  }

  // Handle explicit literals
  if (operand && typeof operand === 'object' && 'value' in operand) {
    return operand.value
  }

  // Handle literal values
  return operand
}

/**
 * Extracts a join key value from a row based on the operand
 * @param row The data row (not nested)
 * @param operand The operand to extract the key from
 * @param defaultTableAlias The default table alias
 * @returns The extracted key value
 */
function extractJoinKey<T extends Record<string, any>>(
  row: T,
  operand: ConditionOperand,
  defaultTableAlias?: string,
): any {
  console.log(`Extracting join key from row:`, JSON.stringify(row));
  console.log(`Using operand:`, JSON.stringify(operand));
  console.log(`Default table alias:`, defaultTableAlias);
  
  let keyValue;
  
  // Handle column references (e.g., "@orders.id" or "@id")
  if (typeof operand === 'string' && operand.startsWith('@')) {
    const columnRef = operand.substring(1);
    
    // If it contains a dot, extract the table and column
    if (columnRef.includes('.')) {
      const [tableAlias, colName] = columnRef.split('.');
      // If this is referencing the current table, extract from row directly
      if (tableAlias === defaultTableAlias) {
        keyValue = row[colName];
      } else {
        // This might be a column from another table, return undefined
        keyValue = undefined;
      }
    } else {
      // No table specified, look directly in the row
      keyValue = row[columnRef];
    }
  } else if (operand && typeof operand === 'object' && 'col' in operand) {
    // Handle explicit column references like { col: "orders.id" } or { col: "id" }
    const colRef = operand.col;
    
    if (typeof colRef === 'string') {
      if (colRef.includes('.')) {
        const [tableAlias, colName] = colRef.split('.');
        // If this is referencing the current table, extract from row directly
        if (tableAlias === defaultTableAlias) {
          keyValue = row[colName];
        } else {
          // This might be a column from another table, return undefined
          keyValue = undefined;
        }
      } else {
        // No table specified, look directly in the row
        keyValue = row[colRef];
      }
    }
  } else {
    // Handle literals or other types
    keyValue = operand;
  }
  
  console.log(`Extracted key value:`, keyValue);
  return keyValue;
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
    case 'not like':
      if (typeof leftValue === 'string' && typeof rightValue === 'string') {
        // Convert SQL LIKE pattern to proper regex pattern
        let pattern = convertLikeToRegex(rightValue)
        const matches = new RegExp(`^${pattern}$`, 'i').test(leftValue)
        return comparator === 'like' ? matches : !matches
      }
      return comparator === 'like' ? false : true
    case 'in':
      // If right value is not an array, we can't do an IN operation
      if (!Array.isArray(rightValue)) {
        return false
      }

      // For empty arrays, nothing is contained in them
      if (rightValue.length === 0) {
        return false
      }

      // Handle array-to-array comparison (check if any element in leftValue exists in rightValue)
      if (Array.isArray(leftValue)) {
        return leftValue.some((item) => isValueInArray(item, rightValue))
      }

      // Handle single value comparison
      return isValueInArray(leftValue, rightValue)

    case 'not in':
      // If right value is not an array, everything is "not in" it
      if (!Array.isArray(rightValue)) {
        return true
      }

      // For empty arrays, everything is "not in" them
      if (rightValue.length === 0) {
        return true
      }

      // Handle array-to-array comparison (check if no element in leftValue exists in rightValue)
      if (Array.isArray(leftValue)) {
        return !leftValue.some((item) => isValueInArray(item, rightValue))
      }

      // Handle single value comparison
      return !isValueInArray(leftValue, rightValue)

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
 * Converts a SQL LIKE pattern to a JavaScript regex pattern
 * @param pattern The SQL LIKE pattern to convert
 * @returns A regex-compatible pattern string
 */
function convertLikeToRegex(pattern: string): string {
  let finalPattern = ''
  let i = 0

  while (i < pattern.length) {
    const char = pattern[i]

    // Handle escape character
    if (char === '\\' && i + 1 < pattern.length) {
      // Add the next character as a literal (escaped)
      finalPattern += pattern[i + 1]
      i += 2 // Skip both the escape and the escaped character
      continue
    }

    // Handle SQL LIKE special characters
    switch (char) {
      case '%':
        // % matches any sequence of characters (including empty)
        finalPattern += '.*'
        break
      case '_':
        // _ matches any single character
        finalPattern += '.'
        break
      // Handle regex special characters
      case '.':
      case '^':
      case '$':
      case '*':
      case '+':
      case '?':
      case '(':
      case ')':
      case '[':
      case ']':
      case '{':
      case '}':
      case '|':
      case '/':
        // Escape regex special characters
        finalPattern += '\\' + char
        break
      default:
        // Regular character, just add it
        finalPattern += char
    }

    i++
  }

  return finalPattern
}

/**
 * Helper function to check if a value is in an array, with special handling for various types
 * @param value The value to check for
 * @param array The array to search in
 * @param caseInsensitive Optional flag to enable case-insensitive matching for strings (default: false)
 * @returns True if the value is found in the array
 */
function isValueInArray(
  value: any,
  array: any[],
  caseInsensitive: boolean = false,
): boolean {
  // Direct inclusion check first (fastest path)
  if (array.includes(value)) {
    return true
  }

  // Handle null/undefined
  if (value === null || value === undefined) {
    return array.some((item) => item === null || item === undefined)
  }

  // Handle numbers and strings with type coercion
  if (typeof value === 'number' || typeof value === 'string') {
    return array.some((item) => {
      // Same type, direct comparison
      if (typeof item === typeof value) {
        if (typeof value === 'string' && caseInsensitive) {
          // Case-insensitive comparison for strings (only if explicitly enabled)
          return (
            (value as string).toLowerCase() === (item as string).toLowerCase()
          )
        }
        return item === value
      }

      // Different types, try coercion for number/string
      if (
        (typeof item === 'number' || typeof item === 'string') &&
        (typeof value === 'number' || typeof value === 'string')
      ) {
        // Convert both to strings for comparison
        return String(item) === String(value)
      }

      return false
    })
  }

  // Handle objects/arrays by comparing stringified versions
  if (typeof value === 'object' && value !== null) {
    const valueStr = JSON.stringify(value)
    return array.some((item) => {
      if (typeof item === 'object' && item !== null) {
        return JSON.stringify(item) === valueStr
      }
      return false
    })
  }

  // Fallback
  return false
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
      
      // First try to get the value using the explicit table.column format
      const explicitKey = `${tableAlias}.${columnName}`;
      if (explicitKey in row) {
        return row[explicitKey];
      }
      
      // Fall back to just the column name
      if (columnName in row) {
        return row[columnName];
      }
      
      // If we still don't have a value, try to find it in any table-prefixed columns
      for (const key in row) {
        if (key.endsWith(`.${columnName}`)) {
          return row[key];
        }
      }
      
      return undefined;
    } else {
      // Simple column reference
      // First check if the column exists directly
      if (columnRef in row) {
        return row[columnRef];
      }
      
      // If not, check if it exists with the default table alias
      if (defaultTableAlias) {
        const prefixedKey = `${defaultTableAlias}.${columnRef}`;
        if (prefixedKey in row) {
          return row[prefixedKey];
        }
      }
      
      // If we still don't have a value, try to find it in any table-prefixed columns
      for (const key in row) {
        if (key.endsWith(`.${columnRef}`)) {
          return row[key];
        }
      }
      
      return row[columnRef]; // Return undefined or whatever is there
    }
  }

  // Handle explicit column references
  if (operand && typeof operand === 'object' && 'col' in operand) {
    const colRef = operand.col

    // Check if it's a table.column reference
    if (typeof colRef === 'string' && colRef.includes('.')) {
      const [tableAlias, columnName] = colRef.split('.')
      
      // First try to get the value using the explicit table.column format
      const explicitKey = `${tableAlias}.${columnName}`;
      if (explicitKey in row) {
        return row[explicitKey];
      }
      
      // Fall back to just the column name
      if (columnName in row) {
        return row[columnName];
      }
      
      // If we still don't have a value, try to find it in any table-prefixed columns
      for (const key in row) {
        if (key.endsWith(`.${columnName}`)) {
          return row[key];
        }
      }
      
      return undefined;
    }

    // Simple column reference
    if (colRef in row) {
      return row[colRef];
    }
    
    // If not, check if it exists with the default table alias
    if (defaultTableAlias && typeof colRef === 'string') {
      const prefixedKey = `${defaultTableAlias}.${colRef}`;
      if (prefixedKey in row) {
        return row[prefixedKey];
      }
    }
    
    return row[colRef]; // Return undefined or whatever is there
  }

  // Handle explicit literals
  if (operand && typeof operand === 'object' && 'value' in operand) {
    return operand.value
  }

  // Handle literal values
  return operand
}
