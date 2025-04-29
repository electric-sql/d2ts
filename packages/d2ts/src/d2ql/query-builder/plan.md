# A query builder for D2QL

The D2QL query builder is a TypeScript library that allows you to build D2QL queries using a fluent API.

The compiler in `d2ql/compiler.ts` works through a `Query` in these steps:

1. Process the `with` clause
2. Process the `from` clause
3. Process the `join` clause
4. Process the `where` clause
5. Process the `groupBy` clause
6. Process the `having` clause
7. Process the `select` clause
8. Process the `keyBy` clause
9. Process the `orderBy`, `limit`, and `offset` clauses

The `QueryBuilder` will expose a method for each of the clauses, and each method will return a new `QueryBuilder` instance. For most, the arguments to each method will be the same as the value it would be given in a `Query` object.

Dependencies:

- a `with` clause can be defined at any point
- all other clauses depend on `from` and cannot be defined untill `from` is defined
- `join` can be called multiple times, once a `from` clause has been defined, this will add a `join` to the query on the first call, and add a new item to the `join` array on each subsequent call
- `where` can be called multiple times, once a `from` clause has been defined, this will add a `where` to the query on the first call, and `AND` the new condition to the existing `where` clause on each subsequent call
- `groupBy` can only be called once. 
- `having` can be called multiple times, once a `from` clause has been defined, this will add a `having` to the query on the first call, and `AND` the new condition to the existing `having` clause on each subsequent call
- `select`, `keyBy`, `orderBy`, `limit`, and `offset` can be called multiple times, overwriting the previous values.

## Type

There is a `Context` type that is intended to be derived/modified by each step of the query builder, so that next step can have the correct type.

When using `from`, it would set the `default` to the table name.

the `Schema` is set on the constructor of `QueryBuilder`.

## `.from()`

```
type FromMethod = {
  (table: string): QueryBuilder
  (table: string, as: string): QueryBuilder
  ({
    table: string,
    as: string,
  }): QueryBuilder
}
```

## `.join()`

```
type JoinMethod = {
  (join: JoinClause): QueryBuilder
}
```

## `.where()`

```
type WhereMethod = {
  (condition: Condition): QueryBuilder
}
```

## `.groupBy()`

```
type GroupByMethod = {
  (groupBy: string | string[]): QueryBuilder
}
```

## `.having()`

```
type HavingMethod = {
  (having: Condition): QueryBuilder
}
```

## `.select()`

```
type Select = string | { [alias: string]: string | FunctionCall }

type SelectMethod = {
  (...select: Select): QueryBuilder
}
```

## `.keyBy()`

```
type KeyByMethod = {
  (keyBy: string | string[]): QueryBuilder
}
```

## `.orderBy()`

```
type OrderByMethod = {
  (orderBy: OrderBy): QueryBuilder
}
```

## `.limit()`

```
type LimitMethod = {
  (limit: number): QueryBuilder
}
```

## `.offset()`

```
type OffsetMethod = {
  (offset: number): QueryBuilder
}
```

## Basic usage

Based on the `kitchen_sink.ts` example, we can build the following query:

```ts
import { QueryBuilder } from '@electric-sql/d2ts/d2ql';

interface Employee {
  id: number
  name: string
  department_id: number | null
  salary: number
  hire_date: string
  active: boolean
  preferences: string // JSON string
}

interface Department {
  id: number
  name: string
  location: string
  budget: number
}

interface MySchema {
  employees: Employee
  departments: Department
}

const query = new QueryBuilder<MySchema>()
  .from('employees')
  .join({
    type: 'inner',
    from: 'departments',
    as: 'd',
    on: ['@e.department_id', '=', '@d.id'],
  })
  .where([
    ['@e.salary', '>', 50000],
    'or',
    ['@d.name', '=', 'Engineering'],
  ])
  .select(
    '@e.id',
    '@e.active',
    {
      emp_name: '@e.name',
      dept_name: '@d.name',
      location: '@d.location',
      upper_name: {
        UPPER: '@e.name',
      },
      annual_salary: '@e.salary',
      theme: {
        JSON_EXTRACT: ['@e.preferences', 'theme'],
      },
      hire_date: {
        DATE: '@e.hire_date',
      },
      employee_info: {
        CONCAT: ['Employee: ', '@e.name'],
      },
    },
  )
```

## With clause

The `with` clause is used to define a common table expression (CTE).

We will use a query builder to build the CTE, and then pass it to the `with` clause.

```ts
const query = new QueryBuilder<MySchema>()
  .with('my_subquery', q => q
    .from('employees')
    .select('@id', '@name')
  )
  .from('my_subquery')
  .select('@id', '@name')
```