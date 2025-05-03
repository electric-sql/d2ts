/**
 * D2QL Joins Example
 * 
 * This example demonstrates the usage of JOIN clauses in D2QL to combine data
 * from multiple sources. It covers all join types: INNER, LEFT, RIGHT, and FULL.
 */

import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts'
import { Query, compileQuery } from '@electric-sql/d2ql'
import { Message, MessageType } from '@electric-sql/d2ts'

// Sample types for examples
type Department = {
  id: number
  name: string
  location: string
}

type Employee = {
  id: number
  name: string
  title: string
  departmentId: number | null
  salary: number
}

// Sample data for examples
const departments: Department[] = [
  { id: 1, name: 'Engineering', location: 'Building A' },
  { id: 2, name: 'Marketing', location: 'Building B' },
  { id: 3, name: 'HR', location: 'Building A' },
  { id: 4, name: 'Finance', location: 'Building C' },
]

const employees: Employee[] = [
  { id: 101, name: 'Alice', title: 'Software Engineer', departmentId: 1, salary: 90000 },
  { id: 102, name: 'Bob', title: 'Marketing Specialist', departmentId: 2, salary: 75000 },
  { id: 103, name: 'Charlie', title: 'HR Manager', departmentId: 3, salary: 85000 },
  { id: 104, name: 'Dave', title: 'Senior Engineer', departmentId: 1, salary: 110000 },
  { id: 105, name: 'Eve', title: 'Graphic Designer', departmentId: 2, salary: 70000 },
  { id: 106, name: 'Frank', title: 'Contractor', departmentId: null, salary: 95000 },
  { id: 107, name: 'Grace', title: 'Intern', departmentId: 1, salary: 45000 },
]

// Example 1: INNER JOIN
console.log("\n=== Example 1: INNER JOIN ===")
{
  // Define a D2QL query with an INNER JOIN
  const query: Query = {
    select: [
      { emp_id: '@employees.id' },
      { emp_name: '@employees.name' },
      { dept_name: '@departments.name' },
      { location: '@departments.location' }
    ],
    from: 'employees',
    as: 'employees',
    join: [
      {
        type: 'inner',
        from: 'departments',
        as: 'departments',
        on: ['@employees.departmentId', '=', '@departments.id']
      }
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
        console.log("INNER JOIN results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the inputs
  employeesInput.sendData(
    v([1, 0]),
    new MultiSet(employees.map((employee) => [employee, 1])),
  )
  employeesInput.sendFrontier(new Antichain([v([1, 0])]))

  departmentsInput.sendData(
    v([1, 0]),
    new MultiSet(departments.map((department) => [department, 1])),
  )
  departmentsInput.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 2: LEFT JOIN
console.log("\n=== Example 2: LEFT JOIN ===")
{
  // Define a D2QL query with a LEFT JOIN
  const query: Query = {
    select: [
      { emp_id: '@employees.id' },
      { emp_name: '@employees.name' },
      { dept_name: '@departments.name' },
      { location: '@departments.location' }
    ],
    from: 'employees',
    as: 'employees',
    join: [
      {
        type: 'left',
        from: 'departments',
        as: 'departments',
        on: ['@employees.departmentId', '=', '@departments.id']
      }
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
        console.log("LEFT JOIN results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the inputs
  employeesInput.sendData(
    v([1, 0]),
    new MultiSet(employees.map((employee) => [employee, 1])),
  )
  employeesInput.sendFrontier(new Antichain([v([1, 0])]))

  departmentsInput.sendData(
    v([1, 0]),
    new MultiSet(departments.map((department) => [department, 1])),
  )
  departmentsInput.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 3: RIGHT JOIN
console.log("\n=== Example 3: RIGHT JOIN ===")
{
  // Define a D2QL query with a RIGHT JOIN
  const query: Query = {
    select: [
      { emp_id: '@employees.id' },
      { emp_name: '@employees.name' },
      { dept_id: '@departments.id' },
      { dept_name: '@departments.name' }
    ],
    from: 'employees',
    as: 'employees',
    join: [
      {
        type: 'right',
        from: 'departments',
        as: 'departments',
        on: ['@employees.departmentId', '=', '@departments.id']
      }
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
        console.log("RIGHT JOIN results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the inputs
  employeesInput.sendData(
    v([1, 0]),
    new MultiSet(employees.map((employee) => [employee, 1])),
  )
  employeesInput.sendFrontier(new Antichain([v([1, 0])]))

  departmentsInput.sendData(
    v([1, 0]),
    new MultiSet(departments.map((department) => [department, 1])),
  )
  departmentsInput.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 4: FULL JOIN
console.log("\n=== Example 4: FULL JOIN ===")
{
  // Define a D2QL query with a FULL JOIN
  const query: Query = {
    select: [
      { emp_id: '@employees.id' },
      { emp_name: '@employees.name' },
      { dept_id: '@departments.id' },
      { dept_name: '@departments.name' }
    ],
    from: 'employees',
    as: 'employees',
    join: [
      {
        type: 'full',
        from: 'departments',
        as: 'departments',
        on: ['@employees.departmentId', '=', '@departments.id']
      }
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
        console.log("FULL JOIN results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the inputs
  employeesInput.sendData(
    v([1, 0]),
    new MultiSet(employees.map((employee) => [employee, 1])),
  )
  employeesInput.sendFrontier(new Antichain([v([1, 0])]))

  departmentsInput.sendData(
    v([1, 0]),
    new MultiSet(departments.map((department) => [department, 1])),
  )
  departmentsInput.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Main execution
if (require.main === module) {
  console.log("Running D2QL joins examples...")
} 