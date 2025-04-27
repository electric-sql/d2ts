import type { Query, Condition, Select, From } from '../schema.js'

import type { Schema, Input, Context, InputReference, MaybeRenameInput } from '../types.js'

/**
 * A query builder for D2QL that provides a fluent API
 * for building type-safe queries.
 */
export class QueryBuilder<C extends Context<Schema>> {
  private readonly query: Partial<Query> = {}

  /**
   * Create a new QueryBuilder instance.
   */
  constructor(query: Partial<Query> = {}) {
    this.query = query
  }

  /**
   * Specify the table to query from.
   * This is the first method that must be called in the chain.
   *
   * @param table The table name to query from
   * @param as Optional alias for the table
   * @returns A new QueryBuilder with the from clause set
   */
  from<T extends InputReference<C>, As extends string>(
    table: T,
    as?: As,
  ): QueryBuilder<{
    schema: MaybeRenameInput<C['schema'], T, As>
    default: T
  }> {
    const newBuilder = new QueryBuilder<{
      schema: MaybeRenameInput<C['schema'], T, As>
      default: T
    }>()
    Object.assign(newBuilder.query, this.query)
    newBuilder.query.from = table as From<C>
    if (as) {
      newBuilder.query.as = as
    }
    return newBuilder
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
  select(
    this: QueryBuilder<C>,
    ...selects: Select<C>[]
  ): this {
    const newBuilder = new QueryBuilder<C>(this.query);
    newBuilder.query.select = selects;
    return newBuilder as this;
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

export function queryBuilder<S extends Schema>() {
  return new QueryBuilder<{
    schema: S
  }>()
}
