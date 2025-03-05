// JSONLike supports any JSON-compatible value plus Date objects.
export type JSONLike =
  | string
  | number
  | boolean
  | Date
  | null
  | JSONLike[]
  | { [key: string]: JSONLike }

// LiteralValue supports common primitives, JS Date, or undefined.
export type LiteralValue = string | number | boolean | Date | null | undefined

// A column reference may be a template literal starting with "@" or an explicit object.
export type ColumnReference = `@${string}` | { col: string }

// To force a literal value (which may be arbitrary JSON or a Date), wrap it in an object with the "value" key.
export interface ExplicitLiteral {
  value: JSONLike
}

// Allowed function names (common SQL functions)
export type AllowedFunctionName =
  | 'DATE'
  | 'JSON_EXTRACT'
  | 'JSON_EXTRACT_PATH'
  | 'UPPER'
  | 'LOWER'
  | 'COALESCE'
  | 'CONCAT'
  | 'LENGTH'

// A function call is represented as a union of objects—each having exactly one key that is one of the allowed function names.
export type FunctionCall = {
  [K in AllowedFunctionName]: {
    [key in K]: ConditionOperand | ConditionOperand[]
  }
}[AllowedFunctionName]

/**
 * An operand in a condition may be:
 * - A literal value (LiteralValue)
 * - A column reference (a string starting with "@" or an explicit { col: string } object)
 * - An explicit literal (to wrap arbitrary JSON or Date values) as { value: ... }
 * - A function call (as defined above)
 * - An array of operands (for example, for "in" clauses)
 */
export type ConditionOperand =
  | LiteralValue
  | ColumnReference
  | ExplicitLiteral
  | FunctionCall
  | ConditionOperand[]

// Allowed SQL comparators.
export type Comparator =
  | '='
  | '!='
  | '<'
  | '<='
  | '>'
  | '>='
  | 'like'
  | 'not like'
  | 'in'
  | 'not in'
  | 'is'
  | 'is not'

// Logical operators.
export type LogicalOperator = 'and' | 'or'

// A simple condition is a tuple: [left operand, comparator, right operand].
export type SimpleCondition = [ConditionOperand, Comparator, ConditionOperand]

// A flat composite condition allows all elements to be at the same level:
// [left1, op1, right1, 'and'/'or', left2, op2, right2, ...]
export type FlatCompositeCondition = [
  ConditionOperand,
  Comparator,
  ConditionOperand,
  ...(LogicalOperator | ConditionOperand | Comparator)[],
]

// A nested composite condition combines conditions with logical operators
// The first element can be a SimpleCondition or FlatCompositeCondition
// followed by logical operators and more conditions
export type NestedCompositeCondition = [
  SimpleCondition | FlatCompositeCondition,
  ...(LogicalOperator | SimpleCondition | FlatCompositeCondition)[],
]

// A condition is either a simple condition or a composite condition (flat or nested).
export type Condition =
  | SimpleCondition
  | FlatCompositeCondition
  | NestedCompositeCondition

// A join clause includes a join type, the table to join, an optional alias,
// an "on" condition, and an optional "where" clause specific to the join.
export interface JoinClause {
  type: 'inner' | 'left' | 'right' | 'full' | 'cross'
  from: string
  as?: string
  on: Condition
  where?: Condition
}

// The orderBy clause can be a string, an object mapping a column to "asc" or "desc",
// or an array of such items.
export type OrderBy =
  | string
  | { [column: string]: 'asc' | 'desc' }
  | Array<string | { [column: string]: 'asc' | 'desc' }>

// The top-level query interface.
export interface Query {
  // The select clause is an array of either plain strings or objects mapping alias names
  // to expressions. Plain strings starting with "@" denote column references.
  // Plain string "@*" denotes all columns from all tables.
  // Plain string "@table.*" denotes all columns from a specific table.
  select: Array<string | { [alias: string]: string | FunctionCall }>
  as?: string
  from: string
  join?: JoinClause[]
  where?: Condition
  // groupBy may be a single column name or an array of column names.
  groupBy?: string | string[]
  having?: Condition
  orderBy?: OrderBy
  limit?: number
  offset?: number
}
