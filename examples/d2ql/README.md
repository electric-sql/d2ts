# D2QL Examples

This directory contains examples that demonstrate the capabilities of D2QL, a SQL-like query language for D2TS.

## Examples

1. **Basic SELECT (basic_select.ts)**  
   A simple example showing how to select specific fields from a dataset.

2. **WHERE Conditions (where_conditions.ts)**  
   Demonstrates filtering data using various comparison operators and logical conditions.

3. **Joins (joins.ts)**  
   Shows how to join multiple datasets together using different join types.

4. **Functions (functions.ts)**  
   Illustrates the use of built-in functions like UPPER, LOWER, LENGTH, CONCAT, DATE, and JSON_EXTRACT.

5. **Kitchen Sink (kitchen_sink.ts)**  
   A comprehensive example that combines multiple D2QL features:
   - Multiple column selection methods:
     - Direct column references without aliases (e.g., '@e.id')
     - Column references with aliases (e.g., { emp_name: '@e.name' })
   - Short, readable table aliases ('e' for employees, 'd' for departments)
   - Function calls in SELECT clauses (UPPER, JSON_EXTRACT, DATE, CONCAT)
   - LEFT JOIN between employees and departments
   - Complex WHERE conditions with AND/OR logic and function calls

## Running the Examples

To run an example, use the following command:

```bash
npx tsx examples/d2ql/EXAMPLE_FILENAME.ts
```

For instance, to run the kitchen sink example:

```bash
npx tsx examples/d2ql/05_kitchen_sink.ts
```

## Current D2QL Features

- Selecting columns with or without aliases
- WHERE clauses with comparison operators, logical conditions (AND/OR), and nested conditions
- JOIN operations (INNER, LEFT, RIGHT, FULL)
- Function calls in SELECT and WHERE clauses:
  - String functions: UPPER, LOWER, LENGTH, CONCAT
  - Date handling with DATE function
  - JSON data extraction with JSON_EXTRACT
  - Null handling with COALESCE

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