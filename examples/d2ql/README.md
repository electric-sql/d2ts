# D2QL Examples

This directory contains examples demonstrating the capabilities of D2QL, a SQL-like query language for D2. These examples showcase how to use D2QL to perform various query operations on datasets within D2 pipelines.

## Examples Overview

1. **Basic Select (01_basic_select.ts)**
   - Demonstrates basic SELECT statements
   - Shows how to select all columns from a table
   - Illustrates how to use column aliases

2. **Where Conditions (02_where_conditions.ts)**
   - Shows how to filter data with simple WHERE clauses
   - Demonstrates compound conditions using AND
   - Illustrates complex nested conditions with AND/OR combinations

3. **Joins (03_joins.ts)**
   - Shows how to join data from multiple sources
   - Covers all join types: INNER, LEFT, RIGHT, and FULL joins
   - Demonstrates how to properly alias tables in join queries

4. **Functions (04_functions.ts)**
   - Demonstrates usage of built-in functions in SELECT clauses
   - Shows how to use functions in WHERE clauses for filtering
   - Covers string functions: UPPER, LOWER, LENGTH, CONCAT
   - Illustrates date handling and COALESCE for null values
   - Shows how to use JSON_EXTRACT for working with JSON data

## Running the Examples

To run an example, use the following command from the project root:

```bash
npx tsx examples/d2ql/01_basic_select.ts
```

Replace the filename with the example you want to run. Each example runs multiple D2QL queries and prints the results to the console.

## Example Data

The examples use sample datasets that simulate common data scenarios:

- Users/customers with profiles and attributes
- Products with categories and pricing
- Employees and departments showing relationships between data

These datasets are created in-memory for each example and don't require any external data sources or database connections.

## D2QL Query Structure

D2QL queries are defined as TypeScript objects with a structure similar to SQL:

```typescript
const query = {
  select: ['@column1', { alias: '@column2' }],
  from: 'table_name',
  where: ['@column', '=', 'value'],
  join: [{
    type: 'inner',
    from: 'other_table',
    on: ['@table.column', '=', '@other_table.column']
  }]
};
```

For more information on D2QL, see the D2QL documentation in the main codebase. 