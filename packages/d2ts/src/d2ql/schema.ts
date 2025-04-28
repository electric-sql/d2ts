import type {
  Context,
  Schema,
  InputReference,
  WildcardReference,
  WildcardReferenceString,
  PropertyReference,
  PropertyReferenceString,
  TypeFromPropertyReference,
} from './types.js'

// Identifires
export type ColumnName<ColumnNames extends string> = ColumnNames

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
// We exclude strings that start with "@" because they are property references.
export type LiteralValue =
  | (string & {})
  | number
  | boolean
  | Date
  | null
  | undefined

// These versions are for use with methods on the query builder where we want to
// ensure that the argument is a string that does not start with "@".
// Can be combined with PropertyReference for validating references.
export type SafeString<T extends string> = T extends `@${string}` ? never : T
export type OptionalSafeString<T extends any> = T extends string
  ? SafeString<T>
  : never
export type LiteralValueWithSafeString<T extends any> =
  | (OptionalSafeString<T> & {})
  | number
  | boolean
  | Date
  | null
  | undefined

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
  | 'ORDER_INDEX'

// A function call is represented as a union of objects—each having exactly one key that is one of the allowed function names.
export type FunctionCall<C extends Context = Context> = {
  [K in AllowedFunctionName]: {
    [key in K]: ConditionOperand<C> | ConditionOperand<C>[]
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
export type ConditionOperand<
  C extends Context = Context,
  T extends any = any,
> =
  | LiteralValue
  | PropertyReference<C>
  | ExplicitLiteral
  | FunctionCall<C>
  | ConditionOperand<C, T>[]

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
export type SimpleCondition<
  C extends Context = Context,
  T extends any = any,
> = [ConditionOperand<C, T>, Comparator, ConditionOperand<C, T>]

// A flat composite condition allows all elements to be at the same level:
// [left1, op1, right1, 'and'/'or', left2, op2, right2, ...]
export type FlatCompositeCondition<
  C extends Context = Context,
  T extends any = any,
> = [
  ConditionOperand<C, T>,
  Comparator,
  ConditionOperand<C, T>,
  ...(LogicalOperator | ConditionOperand<C, T> | Comparator)[],
]

// A nested composite condition combines conditions with logical operators
// The first element can be a SimpleCondition or FlatCompositeCondition
// followed by logical operators and more conditions
export type NestedCompositeCondition<
  C extends Context = Context,
  T extends any = any,
> = [
  SimpleCondition<C, T> | FlatCompositeCondition<C, T>,
  ...(LogicalOperator | SimpleCondition<C, T> | FlatCompositeCondition<C, T>)[],
]

// A condition is either a simple condition or a composite condition (flat or nested).
export type Condition<
  C extends Context = Context,
  T extends any = any,
> = SimpleCondition<C, T>
// | FlatCompositeCondition<C>
// | NestedCompositeCondition<C>

// A join clause includes a join type, the table to join, an optional alias,
// an "on" condition, and an optional "where" clause specific to the join.
export interface JoinClause<C extends Context = Context> {
  type: 'inner' | 'left' | 'right' | 'full' | 'cross'
  from: string
  as?: string
  on: Condition<C>
  where?: Condition<C>
}

// The orderBy clause can be a string, an object mapping a column to "asc" or "desc",
// or an array of such items.
export type OrderBy<C extends Context = Context> =
  | PropertyReferenceString<C>
  | { [column in PropertyReferenceString<C>]?: 'asc' | 'desc' }
  | Record<PropertyReferenceString<C>, 'asc' | 'desc'>
  | Array<
      | PropertyReferenceString<C>
      | { [column in PropertyReferenceString<C>]?: 'asc' | 'desc' }
    >

export type Select<C extends Context = Context> =
  | PropertyReferenceString<C>
  | { [alias: string]: PropertyReference<C> | FunctionCall<C> }
  | WildcardReferenceString<C>

export type As<_C extends Context = Context> = string

export type From<C extends Context = Context> = InputReference<{
  baseSchema: C['baseSchema']
  schema: C['baseSchema']
}>

export type Where<C extends Context = Context> = Condition<C>

export type GroupBy<C extends Context = Context> =
  | PropertyReference<C>
  | PropertyReference<C>[]

export type Having<C extends Context = Context> = Condition<C>

export type Limit<_C extends Context = Context> = number

export type Offset<_C extends Context = Context> = number

export interface BaseQuery<C extends Context = Context> {
  // The select clause is an array of either plain strings or objects mapping alias names
  // to expressions. Plain strings starting with "@" denote column references.
  // Plain string "@*" denotes all columns from all tables.
  // Plain string "@table.*" denotes all columns from a specific table.
  select: Select<C>[]
  as?: As<C>
  from: From<C>
  join?: JoinClause<C>[]
  where?: Condition<C>
  groupBy?: GroupBy<C>
  having?: Condition<C>
  orderBy?: OrderBy<C>
  limit?: Limit<C>
  offset?: Offset<C>
}

// The top-level query interface.
export interface Query<C extends Context = Context> extends BaseQuery<C> {
  keyBy?: PropertyReference<C> | PropertyReference<C>[]
  with?: WithQuery<C>[]
}

// A WithQuery is a query that is used as a Common Table Expression (CTE)
// It cannot be keyed and must have an alias (as)
// There is no support for recursive CTEs
export interface WithQuery<C extends Context = Context> extends BaseQuery<C> {
  as: string
}

// A keyed query is a query that has a keyBy clause, and so the result is always
// a keyed stream.
export interface KeyedQuery<C extends Context = Context> extends Query<C> {
  keyBy: PropertyReference<C> | PropertyReference<C>[]
}

// ====== Some tests during development: ======

interface TestSchema extends Schema {
  users: {
    id: number
    name: string
    email: string
  }
  posts: {
    id: number
    title: string
    content: string
    authorId: number
    views: number
  }
  comments: {
    id: number
    postId: number
    userId: number
    content: string
  }
}

const q: Query<{
  baseSchema: TestSchema
  schema: TestSchema
  default: 'comments'
}> = {
  from: 'comments',
  select: [
    {
      test: '@authorId',
    },
  ],
  orderBy: '@userId',
}

// Test with object-based orderBy
const q2: Query<{
  baseSchema: TestSchema
  schema: TestSchema
  default: 'comments'
}> = {
  from: 'comments',
  select: ['@id'],
  orderBy: '@userId',
}

// We can accomplish sorting direction by adding 'DESC' in the SQL keywords
// This is a common pattern in many SQL-like APIs
const qDesc: Query<{
  baseSchema: TestSchema
  schema: TestSchema
  default: 'comments'
}> = {
  from: 'comments',
  select: [
    '@posts.title',
    '@id',
    {
      test: '@authorId',
    },
  ],
  orderBy: { '@comments.userId': 'desc' },
  groupBy: ['@authorId', { col: 'posts.title' }],
  where: ['@comments.content', '!=', 1],
}
