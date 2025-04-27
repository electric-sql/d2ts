import { queryBuilder } from './query-builder.js';
import type { Schema, Input } from '../types.js';

// Example schema for testing
interface Employee extends Input {
  id: number;
  name: string;
  department_id: number | null;
  salary: number;
  hire_date: string;
  active: boolean;
  preferences: string; // JSON string
}

interface Department extends Input {
  id: number;
  name: string;
  location: string;
  budget: number;
}

// Define schema that conforms to the Schema type
interface TestSchema extends Schema {
  employees: Employee;
  departments: Department;
  [key: string]: Input;  // Index signature required by Schema
}

// Create a query builder with the custom schema
// Using 'employees' as the default table
const query = queryBuilder<TestSchema>()
  .from('employees')
  .select('@active');

// Once we have the 'where' and 'select' methods implemented, we can uncomment these:
// .where(['@salary', '>', 50000])
// .select('@id', '@name', { upper_name: { UPPER: '@name' } });

// Get the built query object
// const builtQuery = query.build();

// Output the query object
// console.log(JSON.stringify(builtQuery, null, 2)); 