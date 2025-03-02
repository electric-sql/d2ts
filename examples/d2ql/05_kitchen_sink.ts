/**
 * D2QL Kitchen Sink Example
 * 
 * This example demonstrates multiple D2QL features in a single query,
 * including joins, function calls, and complex conditions.
 */

import { D2, MultiSet, output, v, Antichain } from '../../packages/d2ts/src/index.js'
import { Query, compileQuery } from '../../packages/d2ts/src/d2ql/index.js'
import { Message, MessageType } from '../../packages/d2ts/src/types.js'

// Define sample types
type Employee = {
  id: number
  name: string
  department_id: number | null
  salary: number
  hire_date: string
  active: boolean
  preferences: string  // JSON string
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
    preferences: '{"theme":"dark","notifications":true}' 
  },
  { 
    id: 2, 
    name: 'Bob Johnson', 
    department_id: 2, 
    salary: 65000, 
    hire_date: '2022-02-10', 
    active: true, 
    preferences: '{"theme":"light","notifications":false}' 
  },
  { 
    id: 3, 
    name: 'Charlie Brown', 
    department_id: 1, 
    salary: 45000, 
    hire_date: '2023-01-20', 
    active: false, 
    preferences: '{"theme":"system","notifications":true}' 
  },
  { 
    id: 4, 
    name: 'Diana Prince', 
    department_id: null, 
    salary: 95000, 
    hire_date: '2020-11-05', 
    active: true, 
    preferences: '{"theme":"dark","notifications":true}' 
  }
]

const departments: Department[] = [
  { id: 1, name: 'Engineering', location: 'Building A', budget: 1000000 },
  { id: 2, name: 'Marketing', location: 'Building B', budget: 500000 },
  { id: 3, name: 'Finance', location: 'Building C', budget: 750000 }
]

console.log("\n=== D2QL Kitchen Sink Example ===")

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
      hire_date: {
        DATE: '@employees.hire_date'
      }
    },
    {
      employee_info: {
        CONCAT: ['Employee: ', '@employees.name']
      }
    }
  ],
  from: 'employees',
  as: 'employees',
  join: [
    {
      type: 'left',  // Using LEFT JOIN to retain employees with no department
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
}

// Create a D2 graph
const graph = new D2({ initialFrontier: v([0, 0]) })

// Create inputs for both tables
const employeesInput = graph.newInput<Employee>()
const departmentsInput = graph.newInput<Department>()

// Compile the query
const pipeline = compileQuery(query, {
  'employees': employeesInput,
  'departments': departmentsInput
})

// Add an output handler
pipeline.pipe(
  output((message: Message<any>) => {
    if (message.type === MessageType.DATA) {
      const results = message.data.collection.getInner().map(([data]) => data)
      console.log("Results:")
      results.forEach((result, index) => {
        console.log(`Record ${index + 1}:`, result)
      })
    }
  }),
)

// Finalize the graph
graph.finalize()

// Send employee data
employeesInput.sendData(
  v([1, 0]),
  new MultiSet(employees.map(employee => [employee, 1])),
)
employeesInput.sendFrontier(new Antichain([v([1, 0])]))

// Send department data
departmentsInput.sendData(
  v([1, 0]),
  new MultiSet(departments.map(department => [department, 1])),
)
departmentsInput.sendFrontier(new Antichain([v([1, 0])]))

// Run the graph
graph.run()

// Description of the query features being demonstrated
console.log("\n=== Features demonstrated in this example ===")
console.log("1. Column selection with aliases")
console.log("2. Multiple function calls in SELECT clauses:")
console.log("   - UPPER: For transforming the employee name to uppercase")
console.log("   - JSON_EXTRACT: For extracting the theme from preferences JSON")
console.log("   - DATE: For converting hire_date string to a Date object")
console.log("   - CONCAT: For combining strings into a single employee_info field")
console.log("3. LEFT JOIN between employees and departments")
console.log("4. Complex WHERE conditions combining:")
console.log("   - AND logic for salary > 50000 AND active = true")
console.log("   - OR logic to include anyone in Engineering regardless of other conditions")
console.log("   - Function call (UPPER) within the WHERE condition")

// Main execution
if (require.main === module) {
  console.log("\nEnd of D2QL kitchen sink example")
} 