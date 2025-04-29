import type {
  Query,
  Condition,
  From,
  LiteralValue,
  Select,
  Comparator,
  JoinClause,
  OrderBy,
  Limit,
  Offset,
  WithQuery,
} from '../schema.js'

import type {
  Schema,
  Context,
  InputReference,
  RemoveIndexSignature,
  PropertyReferenceString,
  PropertyReference,
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
    // Validate function calls in the selects
    // Need to use a type assertion to bypass deep recursive type checking
    const validatedSelects = selects.map((select) => {
      // If the select is an object with aliases, validate each value
      if (
        typeof select === 'object' &&
        select !== null &&
        !Array.isArray(select)
      ) {
        const result: Record<string, any> = {}

        for (const [key, value] of Object.entries(select)) {
          // If it's a function call (object with a single key that is an allowed function name)
          if (
            typeof value === 'object' &&
            value !== null &&
            !Array.isArray(value)
          ) {
            const keys = Object.keys(value)
            if (keys.length === 1) {
              const funcName = keys[0]
              // List of allowed function names from AllowedFunctionName
              const allowedFunctions = [
                'SUM',
                'COUNT',
                'AVG',
                'MIN',
                'MAX',
                'DATE',
                'JSON_EXTRACT',
                'JSON_EXTRACT_PATH',
                'UPPER',
                'LOWER',
                'COALESCE',
                'CONCAT',
                'LENGTH',
                'ORDER_INDEX',
              ]

              if (!allowedFunctions.includes(funcName)) {
                console.warn(
                  `Unsupported function: ${funcName}. Expected one of: ${allowedFunctions.join(', ')}`,
                )
              }
            }
          }

          result[key] = value
        }

        return result
      }

      return select
    })

    const newBuilder = new BaseQueryBuilder<C>(
      (this as BaseQueryBuilder<C>).query,
    )
    newBuilder.query.select = validatedSelects as Select<C>[]

    return newBuilder as QueryBuilder<
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
   * Add a having clause comparing two values.
   * For filtering results after they have been grouped.
   */
  having(
    left: PropertyReferenceString<C> | LiteralValue,
    operator: Comparator,
    right: PropertyReferenceString<C> | LiteralValue,
  ): QueryBuilder<C>

  /**
   * Add a having clause with a complete condition object.
   * For filtering results after they have been grouped.
   */
  having(condition: Condition<C>): QueryBuilder<C>

  /**
   * Add a having clause to filter the grouped results.
   * Can be called multiple times to add AND conditions.
   *
   * @param leftOrCondition The left operand or complete condition
   * @param operator Optional comparison operator
   * @param right Optional right operand
   * @returns A new QueryBuilder with the having clause added
   */
  having(leftOrCondition: any, operator?: any, right?: any): QueryBuilder<C> {
    // Create a new builder with a copy of the current query
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

    if (!newBuilder.query.having) {
      newBuilder.query.having = condition
    } else {
      // Create a composite condition with AND
      // Use any to bypass type checking issues
      const andArray: any = [newBuilder.query.having, 'and', condition]
      newBuilder.query.having = andArray
    }

    return newBuilder as QueryBuilder<C>
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

  /**
   * Add a join clause to the query without specifying an alias.
   * The table name will be used as the default alias.
   *
   * @param joinClause The join configuration object
   * @returns A new QueryBuilder with the join clause added
   */
  join<
    T extends InputReference<{
      baseSchema: C['baseSchema']
      schema: C['baseSchema']
    }>,
  >(joinClause: {
    type: 'inner' | 'left' | 'right' | 'full' | 'cross'
    from: T
    on: Condition<
      Flatten<{
        baseSchema: C['baseSchema']
        schema: C['schema'] & {
          [K in T]: RemoveIndexSignature<C['baseSchema'][T]>
        }
      }>
    >
    where?: Condition<
      Flatten<{
        baseSchema: C['baseSchema']
        schema: { [K in T]: RemoveIndexSignature<C['baseSchema'][T]> }
      }>
    >
  }): QueryBuilder<
    Flatten<
      Omit<C, 'schema'> & {
        schema: C['schema'] & {
          [K in T]: RemoveIndexSignature<C['baseSchema'][T]>
        }
      }
    >
  >

  /**
   * Add a join clause to the query with a specified alias.
   *
   * @param joinClause The join configuration object with alias
   * @returns A new QueryBuilder with the join clause added
   */
  join<
    T extends InputReference<{
      baseSchema: C['baseSchema']
      schema: C['baseSchema']
    }>,
    A extends string,
  >(joinClause: {
    type: 'inner' | 'left' | 'right' | 'full' | 'cross'
    from: T
    as: A
    on: Condition<
      Flatten<{
        baseSchema: C['baseSchema']
        schema: C['schema'] & {
          [K in A]: RemoveIndexSignature<C['baseSchema'][T]>
        }
      }>
    >
    where?: Condition<
      Flatten<{
        baseSchema: C['baseSchema']
        schema: { [K in A]: RemoveIndexSignature<C['baseSchema'][T]> }
      }>
    >
  }): QueryBuilder<
    Flatten<
      Omit<C, 'schema'> & {
        schema: C['schema'] & {
          [K in A]: RemoveIndexSignature<C['baseSchema'][T]>
        }
      }
    >
  >

  /**
   * Add a join clause to the query.
   *
   * @param joinClause The join configuration object
   * @returns A new QueryBuilder with the join clause added
   */
  join<
    T extends InputReference<{
      baseSchema: C['baseSchema']
      schema: C['baseSchema']
    }>,
    A extends string | undefined = undefined,
  >(joinClause: {
    type: 'inner' | 'left' | 'right' | 'full' | 'cross'
    from: T
    as?: A
    on: Condition<
      Flatten<{
        baseSchema: C['baseSchema']
        schema: C['schema'] & {
          [K in A extends undefined ? T : A]: RemoveIndexSignature<
            C['baseSchema'][T]
          >
        }
      }>
    >
    where?: Condition<
      Flatten<{
        baseSchema: C['baseSchema']
        schema: {
          [K in A extends undefined ? T : A]: RemoveIndexSignature<
            C['baseSchema'][T]
          >
        }
      }>
    >
  }): QueryBuilder<any> {
    // Create a new builder with a copy of the current query
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    // Create a copy of the join clause for the query
    const joinClauseCopy = { ...joinClause } as JoinClause<C>

    // Add the join clause to the query
    if (!newBuilder.query.join) {
      newBuilder.query.join = [joinClauseCopy]
    } else {
      newBuilder.query.join = [...newBuilder.query.join, joinClauseCopy]
    }

    // Determine the alias or use the table name as default
    const effectiveAlias = joinClause.as ?? joinClause.from

    // Return the new builder with updated schema type
    // Use type assertion to simplify complex type
    return newBuilder as QueryBuilder<
      Flatten<
        Omit<C, 'schema'> & {
          schema: C['schema'] & {
            [K in typeof effectiveAlias]: C['baseSchema'][T]
          }
        }
      >
    >
  }

  /**
   * Add an orderBy clause to sort the results.
   * Overwrites any previous orderBy clause.
   *
   * @param orderBy The order specification
   * @returns A new QueryBuilder with the orderBy clause set
   */
  orderBy(orderBy: OrderBy<C>): QueryBuilder<C> {
    // Create a new builder with a copy of the current query
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    // Set the orderBy clause
    newBuilder.query.orderBy = orderBy

    return newBuilder as QueryBuilder<C>
  }

  /**
   * Set a limit on the number of results returned.
   *
   * @param limit Maximum number of results to return
   * @returns A new QueryBuilder with the limit set
   */
  limit(limit: Limit<C>): QueryBuilder<C> {
    // Create a new builder with a copy of the current query
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    // Set the limit
    newBuilder.query.limit = limit

    return newBuilder as QueryBuilder<C>
  }

  /**
   * Set an offset to skip a number of results.
   *
   * @param offset Number of results to skip
   * @returns A new QueryBuilder with the offset set
   */
  offset(offset: Offset<C>): QueryBuilder<C> {
    // Create a new builder with a copy of the current query
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    // Set the offset
    newBuilder.query.offset = offset

    return newBuilder as QueryBuilder<C>
  }

  /**
   * Specify which column(s) to use as keys in the output keyed stream.
   *
   * @param keyBy The column(s) to use as keys
   * @returns A new QueryBuilder with the keyBy clause set
   */
  keyBy(keyBy: PropertyReference<C> | PropertyReference<C>[]): QueryBuilder<C> {
    // Create a new builder with a copy of the current query
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    // Set the keyBy clause
    newBuilder.query.keyBy = keyBy

    return newBuilder as QueryBuilder<C>
  }

  /**
   * Add a groupBy clause to group the results by one or more columns.
   *
   * @param groupBy The column(s) to group by
   * @returns A new QueryBuilder with the groupBy clause set
   */
  groupBy(
    groupBy: PropertyReference<C> | PropertyReference<C>[],
  ): QueryBuilder<C> {
    // Create a new builder with a copy of the current query
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    // Set the groupBy clause
    newBuilder.query.groupBy = groupBy

    return newBuilder as QueryBuilder<C>
  }

  /**
   * Define a Common Table Expression (CTE) that can be referenced in the main query.
   * This allows referencing the CTE by name in subsequent from/join clauses.
   * 
   * @param name The name of the CTE 
   * @param queryBuilderCallback A function that builds the CTE query
   * @returns A new QueryBuilder with the CTE added
   */
  with<N extends string, R = Record<string, unknown>>(
    name: N,
    queryBuilderCallback: (
      builder: InitialQueryBuilder<{
        baseSchema: C['baseSchema']
        schema: {}
      }>,
    ) => QueryBuilder<any>,
  ): InitialQueryBuilder<{
    baseSchema: C['baseSchema'] & { [K in N]: R }
    schema: C['schema']
  }> {
    // Create a new builder with a copy of the current query
    const newBuilder = new BaseQueryBuilder<C>()
    Object.assign(newBuilder.query, this.query)

    // Create a new builder for the CTE
    const cteBuilder = new BaseQueryBuilder<{
      baseSchema: C['baseSchema']
      schema: {}
    }>()

    // Get the CTE query from the callback
    const cteQueryBuilder = queryBuilderCallback(
      cteBuilder as InitialQueryBuilder<{
        baseSchema: C['baseSchema']
        schema: {}
      }>,
    )
    
    // Get the query from the builder
    const cteQuery = cteQueryBuilder.buildQuery()

    // Add an 'as' property to the CTE
    const withQuery: WithQuery<any> = {
      ...cteQuery,
      as: name,
    }

    // Add the CTE to the with array
    if (!newBuilder.query.with) {
      newBuilder.query.with = [withQuery]
    } else {
      newBuilder.query.with = [...newBuilder.query.with, withQuery]
    }

    // Use a type cast that simplifies the type structure to avoid recursion
    return newBuilder as unknown as InitialQueryBuilder<{
      baseSchema: C['baseSchema'] & { [K in N]: R }
      schema: C['schema']
    }>
  }
}

type InitialQueryBuilder<C extends Context<Schema>> = Pick<
  BaseQueryBuilder<C>,
  'from' | 'with'
>

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
