# D2QL

D2QL is a subset of SQL, encoded as JSON/TypeScript, that's compiled to a D2TS pipeline. It provides a declarative way to express data transformations using familiar SQL-like syntax.

This is a work in progress, and the syntax is subject to change as we explore the space of SQL-like query languages for D2TS.

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
    '@age', '>', 21
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
- Wildcard selects (`@*` and `@table.*`) to select all columns
- Simple and complex WHERE clauses with AND/OR logic
- JOIN operations (INNER, LEFT, RIGHT, and FULL)
- Function calls in SELECT and WHERE clauses, including:
  - String functions: UPPER, LOWER, LENGTH, CONCAT
  - Date function for handling date values
  - COALESCE for handling null values
  - JSON_EXTRACT for working with JSON data
- GROUP BY and HAVING clauses with aggregate functions:
  - SUM, COUNT, AVG, MIN, MAX, MEDIAN, MODE

## Planned Features

Future versions will support:

- ORDER BY, LIMIT, and OFFSET clauses

## GROUP BY and Aggregation

D2QL supports grouping data and performing aggregate operations using the `GROUP BY` clause. This allows you to summarize data across groups and calculate statistics.

### Basic GROUP BY Example

```typescript
const query: Query = {
  select: [
    '@customer_id',
    { total_amount: { SUM: '@amount' } },
    { order_count: { COUNT: '@order_id' } }
  ],
  from: 'orders',
  groupBy: ['@customer_id']
};
```

This query groups orders by customer_id and calculates the total amount and count of orders for each customer.

### Multiple Columns in GROUP BY

You can group by multiple columns to create more specific groups:

```typescript
const query: Query = {
  select: [
    '@customer_id',
    '@status',
    { total_amount: { SUM: '@amount' } },
    { order_count: { COUNT: '@order_id' } }
  ],
  from: 'orders',
  groupBy: ['@customer_id', '@status']
};
```

This groups orders by both customer_id and status, giving you totals for each combination.

### Using HAVING to Filter Groups

The `HAVING` clause allows you to filter groups based on aggregate values:

```typescript
const query: Query = {
  select: [
    '@customer_id',
    '@status',
    { total_amount: { SUM: '@amount' } },
    { order_count: { COUNT: '@order_id' } }
  ],
  from: 'orders',
  groupBy: ['@customer_id', '@status'],
  having: [{ col: 'total_amount' }, '>', 200]
};
```

This query only returns groups where the total_amount is greater than 200.

### Supported Aggregate Functions

D2QL supports the following aggregate functions:

- **SUM**: Calculates the sum of values in a group
  ```typescript
  { total_amount: { SUM: '@amount' } }
  ```

- **COUNT**: Counts the number of rows in a group
  ```typescript
  { order_count: { COUNT: '@order_id' } }
  ```

- **AVG**: Calculates the average of values in a group
  ```typescript
  { avg_amount: { AVG: '@amount' } }
  ```

- **MIN**: Finds the minimum value in a group
  ```typescript
  { min_amount: { MIN: '@amount' } }
  ```

- **MAX**: Finds the maximum value in a group
  ```typescript
  { max_amount: { MAX: '@amount' } }
  ```

- **MEDIAN**: Calculates the median value in a group
  ```typescript
  { median_amount: { MEDIAN: '@amount' } }
  ```

- **MODE**: Finds the most common value in a group
  ```typescript
  { most_common_status: { MODE: '@status' } }
  ```

### Combining WHERE and GROUP BY

You can use WHERE to filter rows before grouping and HAVING to filter groups after aggregation:

```typescript
const query: Query = {
  select: [
    '@customer_id',
    { total_amount: { SUM: '@amount' } },
    { order_count: { COUNT: '@order_id' } }
  ],
  from: 'orders',
  where: ['@status', '=', 'completed'],
  groupBy: ['@customer_id'],
  having: [{ col: 'total_amount' }, '>', 500]
};
```

This query:
1. Filters to include only completed orders
2. Groups the filtered orders by customer_id
3. Calculates total_amount and order_count for each customer
4. Returns only groups where the total_amount exceeds 500

## Wildcard Selects

D2QL supports wildcard selects to easily retrieve all columns from tables:

```typescript
// Select all columns from all tables
const query1: Query = {
  select: ['@*'],
  from: 'users'
};

// Select all columns from a specific table
const query2: Query = {
  select: ['@users.*'],
  from: 'users',
  as: 'users'
};

// In joins, select all columns from specific tables
const query3: Query = {
  select: [
    '@u.*',                   // All columns from users
    { 'order_id': '@o.id' }   // Just the id from orders
  ],
  from: 'users',
  as: 'u',
  join: [
    {
      type: 'inner',
      from: 'orders',
      as: 'o',
      on: ['@u.id', '=', '@o.userId']
    }
  ]
};
```

## Keyed Streams with keyBy

D2QL supports creating keyed streams using the `keyBy` parameter. This allows you to specify which column(s) to use as keys in the output stream, making it easier to index or look up data by specific keys.

```typescript
// Key by a single column (id)
const query1: Query = {
  select: ['@id', '@name', '@email'],
  from: 'users',
  keyBy: '@id'
};

// Key by multiple columns
const query2: Query = {
  select: ['@id', '@name', '@department', '@role'],
  from: 'employees',
  keyBy: ['@department', '@role']
};
```

## Kitchen Sink Example

Here's a comprehensive example demonstrating many of D2QL's current capabilities in a single query:

```typescript
import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts';
import { Query, compileQuery } from '@electric-sql/d2ts/d2ql';
import { Message, MessageType } from '@electric-sql/d2ts';

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
    // Non-aliased columns (direct references)
    '@e.id',
    '@e.active',
    {
      // Aliased columns
      emp_name: '@e.name',
      dept_name: '@d.name',
      location: '@d.location',
      // Function calls
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
  ],
  from: 'employees',
  as: 'e',  // Short alias for employees table
  join: [
    {
      type: 'left',  // Could be 'inner', 'right', or 'full'
      from: 'departments',
      as: 'd',  // Short alias for departments table
      on: ['@e.department_id', '=', '@d.id']
    }
  ],
  where: [
    ['@e.salary', '>', 50000, 'and', '@e.active', '=', true],
    'or',
    [{
      UPPER: '@d.name'
    }, '=', 'ENGINEERING']
  ]
};

// Example with wildcard select
const wildcardQuery: Query = {
  select: ['@e.*', '@d.name', { budget: '@d.budget' }],
  from: 'employees',
  as: 'e',
  join: [{ type: 'inner', from: 'departments', as: 'd', on: ['@e.department_id', '=', '@d.id'] }],
  where: ['@e.active', '=', true]
};

// Example with GROUP BY and aggregations
const groupByQuery: Query = {
  select: [
    '@d.name',
    { 
      avg_salary: { AVG: '@e.salary' },
      total_salary: { SUM: '@e.salary' },
      employee_count: { COUNT: '@e.id' },
      max_salary: { MAX: '@e.salary' },
      min_salary: { MIN: '@e.salary' }
    }
  ],
  from: 'employees',
  as: 'e',
  join: [
    {
      type: 'inner',
      from: 'departments',
      as: 'd',
      on: ['@e.department_id', '=', '@d.id']
    }
  ],
  where: ['@e.active', '=', true],
  groupBy: ['@d.name'],
  having: [{ col: 'employee_count' }, '>=', 2]
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
1. Multiple column selection methods:
   - Direct column references without aliases (e.g., '@e.id')
   - Column references with aliases (e.g., { emp_name: '@e.name' })
2. Short, readable table aliases ('e' for employees, 'd' for departments)
3. Function calls in SELECT clauses (UPPER, JSON_EXTRACT, DATE)
4. LEFT JOIN between employees and departments
5. Complex WHERE conditions with AND/OR logic and function calls

The GROUP BY example demonstrates:
1. Grouping by department name to analyze employee data by department
2. Multiple aggregate functions (AVG, SUM, COUNT, MAX, MIN) in a single query
3. INNER JOIN to ensure only employees with valid departments are included
4. WHERE clause to filter active employees before grouping
5. HAVING clause to filter groups with at least 2 employees

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
  keyBy?: string | string[];
}
```

See the `schema.ts` file for the complete type definitions. 