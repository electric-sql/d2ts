/**
 * D2QL Kitchen Sink Example
 *
 * This example demonstrates multiple D2QL features in a single query,
 * including joins, function calls, and complex conditions.
 */

import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts'
import { Query, compileQuery } from '@electric-sql/d2ts/d2ql'
import { Message, MessageType } from '@electric-sql/d2ts'

// Define sample types
type Employee = {
  id: number
  name: string
  department_id: number | null
  salary: number
  hire_date: string
  active: boolean
  preferences: string // JSON string
}

type Department = {
  id: number
  name: string
  location: string
  budget: number
}

// Sample data
const employees: Employee[] = [
  {
    id: 1,
    name: 'Alice Smith',
    department_id: 1,
    salary: 85000,
    hire_date: '2021-05-15',
    active: true,
    preferences: '{"theme":"dark","notifications":true}',
  },
  {
    id: 2,
    name: 'Bob Johnson',
    department_id: 2,
    salary: 65000,
    hire_date: '2022-02-10',
    active: true,
    preferences: '{"theme":"light","notifications":false}',
  },
  {
    id: 3,
    name: 'Charlie Brown',
    department_id: 1,
    salary: 45000,
    hire_date: '2023-01-20',
    active: false,
    preferences: '{"theme":"system","notifications":true}',
  },
  {
    id: 4,
    name: 'Diana Prince',
    department_id: null,
    salary: 95000,
    hire_date: '2020-11-05',
    active: true,
    preferences: '{"theme":"dark","notifications":true}',
  },
]

const departments: Department[] = [
  { id: 1, name: 'Engineering', location: 'Building A', budget: 1000000 },
  { id: 2, name: 'Marketing', location: 'Building B', budget: 500000 },
  { id: 3, name: 'Finance', location: 'Building C', budget: 750000 },
]

console.log('\n=== D2QL Kitchen Sink Example ===')

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
  as: 'e',
  join: [
    {
      type: 'left', // Could be 'inner', 'right', or 'full'
      from: 'departments',
      as: 'd',
      on: ['@e.department_id', '=', '@d.id'],
    },
  ],
  where: [
    ['@e.salary', '>', 50000, 'and', '@e.active', '=', true],
    'or',
    [
      {
        UPPER: '@d.name',
      },
      '=',
      'ENGINEERING',
    ],
  ],
}

// Create a D2 graph
const graph = new D2({ initialFrontier: v([0, 0]) })

// Create inputs for both tables
const employeesInput = graph.newInput<Employee>()
const departmentsInput = graph.newInput<Department>()

// Compile the query
const pipeline = compileQuery(query, {
  employees: employeesInput,
  departments: departmentsInput,
})

// Add an output handler
pipeline.pipe(
  output((message: Message<any>) => {
    if (message.type === MessageType.DATA) {
      const results = message.data.collection.getInner().map(([data]) => data)
      console.log('Kitchen Sink Example Results:')
      console.log('-----------------------------')
      console.log('Demonstrates:')
      console.log(
        '1. Multiple column selection methods (aliased and non-aliased)',
      )
      console.log(
        "2. Short, readable table aliases ('e' for employees, 'd' for departments)",
      )
      console.log(
        '3. Function calls in SELECT clauses (UPPER, JSON_EXTRACT, DATE, CONCAT)',
      )
      console.log('4. LEFT JOIN between employees and departments')
      console.log('5. Complex WHERE conditions with AND/OR logic')
      console.log('-----------------------------')
      console.log(results)
    }
  }),
)

// Finalize the graph
graph.finalize()

// Send employee data
employeesInput.sendData(
  v([1, 0]),
  new MultiSet([
    [
      {
        id: 1,
        name: 'Alice Smith',
        department_id: 1,
        salary: 85000,
        hire_date: '2021-05-15',
        active: true,
        preferences: '{"theme":"dark","notifications":true}',
      },
      1,
    ],
    [
      {
        id: 2,
        name: 'Bob Johnson',
        department_id: 2,
        salary: 65000,
        hire_date: '2022-02-10',
        active: true,
        preferences: '{"theme":"light","notifications":false}',
      },
      1,
    ],
    [
      {
        id: 3,
        name: 'Charlie Brown',
        department_id: 1,
        salary: 45000,
        hire_date: '2023-01-20',
        active: false,
        preferences: '{"theme":"system","notifications":true}',
      },
      1,
    ],
    [
      {
        id: 4,
        name: 'Diana Prince',
        department_id: null,
        salary: 95000,
        hire_date: '2021-08-01',
        active: true,
        preferences: '{"theme":"dark","notifications":true}',
      },
      1,
    ],
  ]),
)
employeesInput.sendFrontier(new Antichain([v([1, 0])]))

// Send department data
departmentsInput.sendData(
  v([1, 0]),
  new MultiSet([
    [
      { id: 1, name: 'Engineering', location: 'Building A', budget: 1000000 },
      1,
    ],
    [{ id: 2, name: 'Marketing', location: 'Building B', budget: 500000 }, 1],
    [{ id: 3, name: 'Finance', location: 'Building C', budget: 750000 }, 1],
  ]),
)
departmentsInput.sendFrontier(new Antichain([v([1, 0])]))

// Run the graph
graph.run()

// Description of the query features being demonstrated
console.log('\n=== Features demonstrated in this example ===')
console.log('1. Multiple column selection methods:')
console.log("   - Direct column references without aliases (e.g., '@e.id')")
console.log(
  "   - Column references with aliases (e.g., { emp_name: '@e.name' })",
)
console.log(
  "2. Short, readable table aliases ('e' for employees, 'd' for departments)",
)
console.log('3. Multiple function calls in SELECT clauses:')
console.log('   - UPPER: For transforming text to uppercase')
console.log('   - JSON_EXTRACT: For extracting values from JSON strings')
console.log('   - DATE: For converting string dates to Date objects')
console.log('   - CONCAT: For combining strings')
console.log('4. LEFT JOIN between employees and departments')
console.log('5. Complex WHERE conditions combining:')
console.log('   - AND logic for salary > 50000 AND active = true')
console.log('   - OR logic to include anyone in Engineering')
console.log('   - Function call (UPPER) within the WHERE condition')

// Main execution
if (require.main === module) {
  console.log('\nEnd of D2QL kitchen sink example')
}
