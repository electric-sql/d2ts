# D2QL

D2QL is a subset of SQL, encoded as JSON/TypeScript, that's compiled to a D2 pipeline. It provides a declarative way to express data transformations using familiar SQL-like syntax.

## Basic Usage

```typescript
import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts';
import { Query, compileQuery } from '@electric-sql/d2ts/d2ql';

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
const input = graph.newInput<User>();
const pipeline = compileQuery(query, { [query.from]: input });

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
input.sendFrontier(new Antichain([v([1, 0])]));

// Run the graph
graph.run();
```

## Current Features

The current implementation supports:

- Selecting from a single input
- Selecting columns (with or without aliases)
- Simple and complex WHERE clauses with AND/OR logic
- JOIN operations (INNER, LEFT, RIGHT, and FULL)
- Function calls in SELECT and WHERE clauses, including:
  - String functions: UPPER, LOWER, LENGTH, CONCAT
  - Date function for handling date values
  - COALESCE for handling null values
  - JSON_EXTRACT for working with JSON data

## Planned Features

Future versions will support:

- GROUP BY and HAVING clauses
- ORDER BY, LIMIT, and OFFSET clauses

## Kitchen Sink Example

Here's a comprehensive example demonstrating many of D2QL's current capabilities in a single query:

```typescript
import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts';
import { Query, compileQuery } from '@electric-sql/d2ts/d2ql';
import { Message, MessageType } from '@electric-sql/d2ts/types';

// Define sample types
type Employee = {
  id: number
  name: string
  department_id: number | null
  salary: number
  hire_date: string
  active: boolean
  preferences: string  // JSON string
};

type Department = {
  id: number
  name: string
  location: string
  budget: number
};

// Create a D2QL query with multiple features
const query: Query = {
  select: [
    { emp_id: '@employees.id' },
    { 
      emp_name: { 
        UPPER: '@employees.name' 
      } 
    },
    { dept_name: '@departments.name' },
    { location: '@departments.location' },
    { 
      annual_salary: '@employees.salary' 
    },
    {
      theme: {
        JSON_EXTRACT: ['@employees.preferences', 'theme']
      }
    },
    {
      hire_year: {
        DATE: '@employees.hire_date'
      }
    }
  ],
  from: 'employees',
  as: 'employees',
  join: [
    {
      type: 'left',  // Could be 'inner', 'right', or 'full'
      from: 'departments',
      as: 'departments',
      on: ['@employees.department_id', '=', '@departments.id']
    }
  ],
  where: [
    ['@employees.salary', '>', 50000, 'and', '@employees.active', '=', true],
    'or',
    [{
      UPPER: '@departments.name'
    }, '=', 'ENGINEERING']
  ]
};

// Create a D2 graph
const graph = new D2({ initialFrontier: v([0, 0]) });

// Create inputs for both tables
const employeesInput = graph.newInput<Employee>();
const departmentsInput = graph.newInput<Department>();

// Compile the query
const pipeline = compileQuery(query, {
  'employees': employeesInput,
  'departments': departmentsInput
});

// Add an output handler
pipeline.pipe(
  output((message: Message<any>) => {
    if (message.type === MessageType.DATA) {
      const results = message.data.collection.getInner().map(([data]) => data);
      console.log("Results:", results);
    }
  })
);

// Finalize the graph
graph.finalize();

// Send employee data
employeesInput.sendData(
  v([1, 0]),
  new MultiSet([
    [{ 
      id: 1, 
      name: 'Alice Smith', 
      department_id: 1, 
      salary: 85000, 
      hire_date: '2021-05-15', 
      active: true, 
      preferences: '{"theme":"dark","notifications":true}' 
    }, 1],
    [{ 
      id: 2, 
      name: 'Bob Johnson', 
      department_id: 2, 
      salary: 65000, 
      hire_date: '2022-02-10', 
      active: true, 
      preferences: '{"theme":"light","notifications":false}' 
    }, 1],
    [{ 
      id: 3, 
      name: 'Charlie Brown', 
      department_id: 1, 
      salary: 45000, 
      hire_date: '2023-01-20', 
      active: false, 
      preferences: '{"theme":"system","notifications":true}' 
    }, 1]
  ])
);
employeesInput.sendFrontier(new Antichain([v([1, 0])]));

// Send department data
departmentsInput.sendData(
  v([1, 0]),
  new MultiSet([
    [{ id: 1, name: 'Engineering', location: 'Building A', budget: 1000000 }, 1],
    [{ id: 2, name: 'Marketing', location: 'Building B', budget: 500000 }, 1],
    [{ id: 3, name: 'Finance', location: 'Building C', budget: 750000 }, 1]
  ])
);
departmentsInput.sendFrontier(new Antichain([v([1, 0])]));

// Run the graph
graph.run();
```

This example shows:
1. Column selection with aliases
2. Function calls in SELECT clauses (UPPER, JSON_EXTRACT, DATE)
3. LEFT JOIN between employees and departments
4. Complex WHERE conditions with AND/OR logic and function calls

## Query Schema

D2QL queries are defined using TypeScript types. The main `Query` type has the following structure:

```typescript
interface Query {
  select: Array<string | { [alias: string]: string | FunctionCall }>;
  from: string;
  as?: string;
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