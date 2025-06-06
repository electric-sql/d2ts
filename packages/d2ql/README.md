# D2QL

**This is an experimental package**

D2QL is a subset of SQL, encoded as JSON/TypeScript, that's compiled to a D2TS pipeline. It provides a declarative way to express data transformations using familiar SQL-like syntax.

This is a work in progress, and the syntax is subject to change as we explore the space of SQL-like query languages for D2TS.

## Basic Usage

```typescript
import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts';
import { Query, compileQuery } from '@electric-sql/d2ql';

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
- Common Table Expressions (CTEs) using the WITH clause
- ORDER BY, LIMIT, and OFFSET for sorting and pagination

## Common Table Expressions (CTEs)

D2QL supports SQL-like Common Table Expressions (CTEs) using the `WITH` clause. CTEs allow you to define temporary result sets that can be referenced in the main query or in other CTEs.

### Basic CTE Example

```typescript
const query: Query = {
  with: [
    {
      select: ['@id', '@name', '@age'],
      from: 'users',
      where: ['@age', '>', 21],
      as: 'adult_users'
    }
  ],
  select: ['@id', '@name'],
  from: 'adult_users',
  where: ['@name', 'like', 'A%']
};
```

### Multiple CTEs Example

You can define multiple CTEs and reference earlier CTEs in later ones:

```typescript
const query: Query = {
  with: [
    {
      select: ['@id', '@name', '@age'],
      from: 'users',
      where: ['@age', '>', 21],
      as: 'adult_users'
    },
    {
      select: ['@id', '@name', { order_count: 'COUNT(@order_id)' }],
      from: 'adult_users',
      join: [
        {
          type: 'left',
          from: 'orders',
          on: ['@adult_users.id', '=', '@orders.user_id']
        }
      ],
      groupBy: ['@id', '@name'],
      as: 'user_order_counts'
    }
  ],
  select: ['@id', '@name', '@order_count'],
  from: 'user_order_counts',
  where: ['@order_count', '>', 0]
};
```

### CTE Requirements

- Each CTE must have an `as` property that defines its name
- CTEs cannot have a `keyBy` property (they cannot be keyed)
- CTEs can reference other CTEs defined earlier in the `with` array

## Planned Features

Future versions may support additional SQL features and optimizations.

## ORDER BY, LIMIT, and OFFSET

D2QL now supports ordering and limiting results using the `ORDER BY`, `LIMIT`, and `OFFSET` clauses.

### Basic ORDER BY Example

```typescript
const query: Query = {
  select: [
    '@id',
    '@name',
    { age_in_years: '@age' }
  ],
  from: 'users',
  where: [
    '@age', '>', 21
  ],
  orderBy: '@age',  // Order by age in ascending order
  limit: 10,        // Return only the first 10 results
  offset: 5         // Skip the first 5 results
};
```

### Multiple Columns and Direction in ORDER BY

You can order by multiple columns and specify the direction (ascending or descending):

```typescript
const query: Query = {
  select: ['@id', '@name', '@age'],
  from: 'users',
  orderBy: [
    '@name',                // Order by name in ascending order
    { '@age': 'desc' }      // Then by age in descending order
  ]
};
```

### Using Function Calls in ORDER BY

You can use function calls in the `orderBy` clause for more complex ordering logic:

```typescript
const query: Query = {
  select: ['@id', '@name', '@email'],
  from: 'users',
  orderBy: [
    { 'UPPER': '@name' },   // Order by uppercase name (case-insensitive sort)
    { '@email': 'desc' }    // Then by email in descending order
  ]
};
```

This allows for powerful sorting capabilities like case-insensitive sorting, ordering by date components, or custom transformations.

### Including the Row Index in Results

You can include the row index in the results by using the `ORDER_INDEX` function in the `SELECT` clause:

```typescript
const query: Query = {
  select: [
    '@id',
    '@name',
    { age_in_years: '@age' },
    { index: { 'ORDER_INDEX': 'numeric' } }  // Include the numeric index
  ],
  from: 'users',
  orderBy: '@age'
};
```

The `ORDER_INDEX` function supports the following arguments:
- `'numeric'` or `'default'` or `true`: Returns a numeric index (0, 1, 2, ...)
- `'fractional'`: Returns a fractional index (useful for maintaining stable ordering when inserting items between existing ones)

### LIMIT and OFFSET Requirements

In D2QL, `LIMIT` and `OFFSET` clauses require an `ORDER BY` clause to ensure deterministic results:

```typescript
// This is valid - has both ORDER BY and LIMIT/OFFSET
const query1: Query = {
  select: ['@id', '@name', '@age'],
  from: 'users',
  orderBy: '@id',
  limit: 10,
  offset: 5
};

// This would throw an error - LIMIT without ORDER BY
const query2: Query = {
  select: ['@id', '@name', '@age'],
  from: 'users',
  limit: 10  // Error: LIMIT requires an ORDER BY clause
};
```

Attempting to use `LIMIT` or `OFFSET` without an `ORDER BY` clause will result in an error with the message: "LIMIT and OFFSET require an ORDER BY clause to ensure deterministic results".

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
import { Query, compileQuery } from '@electric-sql/d2ql';
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

## Query Builder

D2QL provides a fluent query builder API that makes it easier to create type-safe queries. The query builder offers a more IDE-friendly approach with better type checking compared to manually creating the query objects.

### Basic Usage

```typescript
import { queryBuilder } from '@electric-sql/d2ql/query-builder';
import { Schema, Input } from '@electric-sql/d2ql/types';

// Define your schema types
interface Employee extends Input {
  id: number
  name: string
  department_id: number
  salary: number
}

interface Department extends Input {
  id: number
  name: string
  budget: number
}

// Define your schema
interface MySchema extends Schema {
  employees: Employee
  departments: Department
}

// Create a query using the builder
const query = queryBuilder<MySchema>()
  .from('employees')
  .select('@id', '@name', { salary_amount: '@salary' })
  .where('@salary', '>', 50000)
  .buildQuery();

// Use the query with compileQuery as normal
const pipeline = compileQuery(query, { [query.from]: employeesInput });
```

### Table Aliases

You can specify table aliases to make your queries more readable:

```typescript
const query = queryBuilder<MySchema>()
  .from('employees', 'e')  // 'e' is the alias
  .select('@e.id', '@e.name')
  .where('@e.salary', '>', 50000)
  .buildQuery();
```

### Joins

The query builder makes JOIN operations easier to construct:

```typescript
const query = queryBuilder<MySchema>()
  .from('employees', 'e')
  .join({
    type: 'inner',
    from: 'departments', 
    as: 'd',
    on: ['@e.department_id', '=', '@d.id']
  })
  .select('@e.id', '@e.name', '@d.name')
  .where('@d.budget', '>', 1000000)
  .buildQuery();
```

### Complex Where Conditions

You can build complex WHERE conditions by chaining multiple where calls, which automatically combines them with AND:

```typescript
const query = queryBuilder<MySchema>()
  .from('employees', 'e')
  .where('@e.salary', '>', 50000)
  .where('@e.department_id', '=', 1)
  .buildQuery();

// Results in where: [['@e.salary', '>', 50000], 'and', ['@e.department_id', '=', 1]]
```

### GROUP BY and HAVING

The query builder makes it easy to create aggregation queries:

```typescript
const query = queryBuilder<MySchema>()
  .from('employees', 'e')
  .join({
    type: 'inner',
    from: 'departments',
    as: 'd',
    on: ['@e.department_id', '=', '@d.id']
  })
  .select(
    '@d.name', 
    { avg_salary: { AVG: '@e.salary' } },
    { employee_count: { COUNT: '@e.id' } }
  )
  .groupBy('@d.name')
  .having({ COUNT: '@e.id' }, '>', 5)
  .buildQuery();
```

### ORDER BY, LIMIT, and OFFSET

You can sort and paginate your results:

```typescript
const query = queryBuilder<MySchema>()
  .from('employees')
  .select('@id', '@name', '@salary')
  .orderBy(['@salary', { '@name': 'asc' }])  // Order by salary, then by name ascending
  .limit(10)  // Return only 10 records
  .offset(20) // Skip the first 20 records
  .buildQuery();
```

### Common Table Expressions (WITH)

You can define and use Common Table Expressions with the `with` method:

```typescript
const query = queryBuilder<MySchema>()
  .with('high_salary_employees', q => 
    q.from('employees')
     .where('@salary', '>', 80000)
     .select('@id', '@name', '@salary')
  )
  .with('dept_with_high_salary', q =>
    q.from('high_salary_employees', 'e')
     .join({
       type: 'inner',
       from: 'departments',
       as: 'd',
       on: ['@e.department_id', '=', '@d.id']
     })
     .select('@d.id', '@d.name', { emp_count: { COUNT: '@e.id' } })
     .groupBy('@d.id', '@d.name')
  )
  .from('dept_with_high_salary')
  .select('@id', '@name', '@emp_count')
  .where('@emp_count', '>', 3)
  .buildQuery();
```

### Keyed Streams with keyBy

Define keys for your output streams:

```typescript
const query = queryBuilder<MySchema>()
  .from('employees')
  .select('@id', '@name', '@department_id')
  .keyBy('@id')  // Use id as the key
  .buildQuery();

// Use multiple keys
const multiKeyQuery = queryBuilder<MySchema>()
  .from('employees')
  .select('@id', '@name', '@department_id')
  .keyBy(['@department_id', '@id'])
  .buildQuery();
```

The query builder provides complete type checking throughout the chain, ensuring that column references, function calls, and other parameters are valid for your schema. 