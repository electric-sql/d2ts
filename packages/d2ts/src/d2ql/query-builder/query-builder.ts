import type {
  Query,
  Condition,
  From,
  LiteralValue,
  Select,
  Comparator,
} from '../schema.js'

import type {
  Schema,
  Context,
  InputReference,
  RemoveIndexSignature,
  PropertyReferenceString,
  Flatten,
  InferResultTypeFromSelectTuple,
} from '../types.js'

/**
 * A query builder for D2QL that provides a fluent API
 * for building type-safe queries.
 */
class BaseQueryBuilder<C extends Context<Schema>> {
  private readonly query: Partial<Query<C>> = {}

  /**
   * Create a new QueryBuilder instance.
   */
  constructor(query: Partial<Query<C>> = {}) {
    this.query = query
  }

  from<
    T extends InputReference<{
      baseSchema: C['baseSchema']
      schema: C['baseSchema']
    }>,
  >(
    table: T,
  ): QueryBuilder<{
    baseSchema: C['baseSchema']
    schema: {
      [K in T]: RemoveIndexSignature<C['baseSchema'][T]>
    }
    default: T
  }>

  from<
    T extends InputReference<{
      baseSchema: C['baseSchema']
      schema: C['baseSchema']
    }>,
    As extends string,
  >(
    table: T,
    as: As,
  ): QueryBuilder<{
    baseSchema: C['baseSchema']
    schema: {
      [K in As]: RemoveIndexSignature<C['baseSchema'][T]>
    }
    default: As
  }>

  /**
   * Specify the table to query from.
   * This is the first method that must be called in the chain.
   *
   * @param table The table name to query from
   * @param as Optional alias for the table
   * @returns A new QueryBuilder with the from clause set
   */
  from<
    T extends InputReference<{
      baseSchema: C['baseSchema']
      schema: C['baseSchema']
    }>,
    As extends string | undefined,
  >(table: T, as?: As) {
    const newBuilder = new BaseQueryBuilder()
    Object.assign(newBuilder.query, this.query)
    newBuilder.query.from = table as From<C>
    if (as) {
      newBuilder.query.as = as
    }

    // Calculate the result type without deep nesting
    type ResultSchema = As extends undefined
      ? { [K in T]: C['baseSchema'][T] }
      : { [K in string & As]: C['baseSchema'][T] }

    type ResultDefault = As extends undefined ? T : string & As

    // Use simpler type assertion to avoid excessive depth
    return newBuilder as unknown as QueryBuilder<{
      baseSchema: C['baseSchema']
      schema: ResultSchema
      default: ResultDefault
    }>
  }

  /**
   * Specify what columns to select.
   * Overwrites any previous select clause.
   *
   * @param selects The columns to select
   * @returns A new QueryBuilder with the select clause set
   */
  select<S extends Select<C>[]>(this: QueryBuilder<C>, ...selects: S) {
    const newBuilder = new BaseQueryBuilder<C>(
      (this as BaseQueryBuilder<C>).query,
    )
    newBuilder.query.select = selects

    return newBuilder as unknown as QueryBuilder<
      Flatten<
        Omit<C, 'result'> & {
          result: InferResultTypeFromSelectTuple<C, S>
        }
      >
    >
  }

  /**
   * Add a where clause comparing two values.
   */
  where(
    left: PropertyReferenceString<C> | LiteralValue,
    operator: Comparator,
    right: PropertyReferenceString<C> | LiteralValue,
  ): QueryBuilder<C>

  /**
   * Add a where clause with a complete condition object.
   */
  where(condition: Condition<C>): QueryBuilder<C>

  /**
   * Add a where clause to filter the results.
   * Can be called multiple times to add AND conditions.
   *
   * @param leftOrCondition The left operand or complete condition
   * @param operator Optional comparison operator
   * @param right Optional right operand
   * @returns A new QueryBuilder with the where clause added
   */
  where(leftOrCondition: any, operator?: any, right?: any): QueryBuilder<C> {
    // Create a new builder with a copy of the current query
    // Use simplistic approach to avoid deep type errors
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    let condition: any

    // Determine if this is a complete condition or individual parts
    if (operator !== undefined && right !== undefined) {
      // Create a condition from parts
      condition = [leftOrCondition, operator, right]
    } else {
      // Use the provided condition directly
      condition = leftOrCondition
    }

    if (!newBuilder.query.where) {
      newBuilder.query.where = condition
    } else {
      // Create a composite condition with AND
      // Use any to bypass type checking issues
      const andArray: any = [newBuilder.query.where, 'and', condition]
      newBuilder.query.where = andArray
    }

    return newBuilder as unknown as QueryBuilder<C>
  }

  /**
   * Build and return the final query object.
   *
   * @returns The built query
   */
  buildQuery(): Query<C> {
    // Create a copy of the query to avoid exposing the internal state directly
    return { ...this.query } as Query<C>
  }
}

type InitialQueryBuilder<C extends Context<Schema>> = Pick<
  BaseQueryBuilder<C>,
  'from'
> // TODO: add 'with' when implemented

type QueryBuilder<C extends Context<Schema>> = Omit<BaseQueryBuilder<C>, 'from'>

/**
 * Create a new query builder with the given schema
 */
export function queryBuilder<B extends Schema>() {
  return new BaseQueryBuilder<{
    baseSchema: B
    schema: {}
  }>() as InitialQueryBuilder<{
    baseSchema: B
    schema: {}
  }>
}

export type ResultFromQueryBuilder<QB> = Flatten<
  QB extends QueryBuilder<infer C>
    ? C extends { result: infer R }
      ? R
      : never
    : never
>
