import type { Query, Condition, Select, From } from '../schema.js'

import type {
  Schema,
  Input,
  Context,
  InputReference,
  MaybeRenameInput,
  RemoveIndexSignature,
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
    return newBuilder as QueryBuilder<{
      baseSchema: C['baseSchema']
      schema: As extends undefined
        ? {
            [K in T]: RemoveIndexSignature<C['baseSchema'][T]>
          }
        : As extends string
          ? {
              [K in As]: RemoveIndexSignature<C['baseSchema'][T]>
            }
          : never
      default: { [K in T]: K }[T]
    }>
  }

  // /**
  //  * Add a where clause to filter the results.
  //  * Can be called multiple times to add AND conditions.
  //  *
  //  * @param condition The condition to filter by
  //  * @returns A new QueryBuilder with the where clause added
  //  */
  // where(
  //   this: QueryBuilder<S, DefaultTable>,
  //   condition: Condition
  // ): this {
  //   const newBuilder = new QueryBuilder<S, DefaultTable>();
  //   Object.assign(newBuilder.query, this.query);

  //   if (!newBuilder.query.where) {
  //     newBuilder.query.where = condition;
  //   } else {
  //     // Add as AND condition
  //     newBuilder.query.where = [
  //       newBuilder.query.where,
  //       'and',
  //       condition
  //     ] as any;
  //   }

  //   return newBuilder as this;
  // }

  /**
   * Specify what columns to select.
   * Overwrites any previous select clause.
   *
   * @param selects The columns to select
   * @returns A new QueryBuilder with the select clause set
   */
  select(this: QueryBuilder<C>, ...selects: Select<C>[]): this {
    const newBuilder = new BaseQueryBuilder<C>(
      (this as BaseQueryBuilder<C>).query,
    )
    newBuilder.query.select = selects
    return newBuilder as this
  }

  // /**
  //  * Build and return the final query object.
  //  *
  //  * @returns The built query
  //  */
  // build(): Query {
  //   if (!this.query.from) {
  //     throw new Error('Query must have a from clause');
  //   }
  //   if (!this.query.select || this.query.select.length === 0) {
  //     throw new Error('Query must have a select clause');
  //   }
  //   return this.query as Query;
  // }
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
