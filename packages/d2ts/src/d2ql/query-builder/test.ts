import { Input, Schema } from '../types.js'
import { queryBuilder, ResultFromQueryBuilder } from './query-builder.js'

// Example schema for testing
interface Employee extends Input {
  id: number
  name: string
  department_id: number | null
  salary: number
  hire_date: string
  active: boolean
  preferences: string // JSON string
}

interface Department extends Input {
  id: number
  name: string
  location: string
  budget: number
}

// Define schema that conforms to the Schema type
interface TestSchema extends Schema {
  employees: Employee
  departments: Department
}

// Create a query builder with the custom schema
// Using 'employees' as the default table
const query = queryBuilder<TestSchema>()
  .from('departments', 'd')
  .where('@d.budget', '>', 1000)
  .select('@id', '@d.budget', { upper_name: { UPPER: '@d.name' } })

type Result = ResultFromQueryBuilder<typeof query>


// Once we have the 'where' and 'select' methods implemented, we can uncomment these:
// .where(['@salary', '>', 50000])
// .select('@id', '@name', { upper_name: { UPPER: '@name' } });

// Get the built query object
// const builtQuery = query.build();

// Output the query object
// console.log(JSON.stringify(builtQuery, null, 2));
