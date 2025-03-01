# D2QL

D2QL is a subset of SQL, encoded as JSON/TypeScript, that's compiled to a D2 pipeline. It provides a declarative way to express data transformations using familiar SQL-like syntax.

## Basic Usage

```typescript
import { D2, MultiSet, output, v } from '@electric-sql/d2ts';
import { Query, createPipeline } from '@electric-sql/d2ts/d2ql';

// Define a D2QL query
const query: Query = {
  select: [
    '@id',
    '@name',
    { age_in_years: '@age' }
  ],
  from: 'users',
  where: [
    '@age',
    '>',
    21
  ]
};

// Create a D2 graph
const graph = new D2({ initialFrontier: v([0, 0]) });

// Create a pipeline from the query
const [input, pipeline] = createPipeline<User>(graph, query);

// Add an output handler
pipeline.pipe(
  output((message) => {
    console.log(message);
  })
);

// Finalize the graph
graph.finalize();

// Send data to the input
input.sendData(
  v([1, 0]),
  new MultiSet([
    [{ id: 1, name: 'Alice', age: 25, email: 'alice@example.com' }, 1],
    [{ id: 2, name: 'Bob', age: 19, email: 'bob@example.com' }, 1],
    [{ id: 3, name: 'Charlie', age: 30, email: 'charlie@example.com' }, 1]
  ])
);

// Send frontier
input.sendFrontier(v([1, 0]));

// Run the graph
graph.run();
```

## Current Features

The current implementation supports:

- Selecting from a single input
- Selecting columns (with or without aliases)
- Basic WHERE clauses that compare a column to a literal

## Planned Features

Future versions will support:

- References to other columns in WHERE clauses
- JOIN operations
- GROUP BY and HAVING clauses
- Function calls in SELECT and WHERE clauses
- ORDER BY, LIMIT, and OFFSET clauses

## Query Schema

D2QL queries are defined using TypeScript types. The main `Query` type has the following structure:

```typescript
interface Query {
  select: Array<string | { [alias: string]: string | FunctionCall }>;
  from: string;
  join?: JoinClause[];
  where?: Condition;
  groupBy?: string | string[];
  having?: Condition;
  orderBy?: OrderBy;
  limit?: number;
  offset?: number;
}
```

See the `schema.ts` file for the complete type definitions. 