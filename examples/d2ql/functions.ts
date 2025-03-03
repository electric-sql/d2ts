/**
 * D2QL Functions Example
 * 
 * This example demonstrates the usage of functions in D2QL for transforming data
 * in SELECT clauses and within WHERE conditions.
 */

import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts'
import { Query, compileQuery } from '@electric-sql/d2ts/d2ql'
import { Message, MessageType } from '@electric-sql/d2ts'

// Sample type for examples
type Customer = {
  id: number
  name: string
  email: string
  registration_date: string
  last_login: string | null
  preferences: string // JSON string
  status: string
}

// Sample data for examples
const sampleCustomers: Customer[] = [
  {
    id: 1,
    name: 'alice smith',
    email: 'alice@example.com',
    registration_date: '2022-03-15',
    last_login: '2023-07-10',
    preferences: '{"theme":"dark","notifications":true,"language":"en"}',
    status: 'active',
  },
  {
    id: 2,
    name: 'BOB JOHNSON',
    email: 'bob@example.com',
    registration_date: '2022-05-20',
    last_login: '2023-06-25',
    preferences: '{"theme":"light","notifications":false,"language":"fr"}',
    status: 'active',
  },
  {
    id: 3,
    name: 'Charlie Brown',
    email: 'charlie@example.com',
    registration_date: '2021-11-08',
    last_login: '2023-01-15',
    preferences: '{"theme":"system","notifications":true,"language":"en"}',
    status: 'inactive',
  },
  {
    id: 4,
    name: 'Diana Prince',
    email: 'diana@example.com',
    registration_date: '2023-01-30',
    last_login: null,
    preferences: '{"theme":"dark","notifications":true,"language":"es"}',
    status: 'pending',
  },
]

// Example 1: String functions (UPPER and LOWER)
console.log("\n=== Example 1: String functions (UPPER and LOWER) ===")
{
  // Define a D2QL query using string functions
  const query: Query = {
    select: [
      '@id',
      { 
        name_upper: { 
          UPPER: '@name' 
        } 
      },
      { 
        email_lower: { 
          LOWER: '@email' 
        } 
      }
    ],
    from: 'customers',
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Customer>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("String function results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleCustomers.map((customer) => [customer, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 2: Date function and COALESCE
console.log("\n=== Example 2: Date function and COALESCE ===")
{
  // Define a D2QL query using DATE function and COALESCE
  const query: Query = {
    select: [
      '@id',
      '@name',
      { 
        registration: { 
          DATE: '@registration_date' 
        } 
      },
      { 
        last_seen: { 
          COALESCE: ['@last_login', '@registration_date'] 
        } 
      }
    ],
    from: 'customers',
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Customer>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("Date and COALESCE results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleCustomers.map((customer) => [customer, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 3: JSON_EXTRACT function
console.log("\n=== Example 3: JSON_EXTRACT function ===")
{
  // Define a D2QL query using JSON_EXTRACT
  const query: Query = {
    select: [
      '@id',
      '@name',
      { 
        theme: { 
          JSON_EXTRACT: ['@preferences', 'theme'] 
        } 
      },
      { 
        language: { 
          JSON_EXTRACT: ['@preferences', 'language'] 
        } 
      }
    ],
    from: 'customers',
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Customer>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("JSON_EXTRACT results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleCustomers.map((customer) => [customer, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 4: Functions in WHERE clauses
console.log("\n=== Example 4: Functions in WHERE clauses ===")
{
  // Define a D2QL query using functions in WHERE clauses
  const query: Query = {
    select: [
      '@id',
      '@name',
      '@email',
      '@status'
    ],
    from: 'customers',
    where: [
      { 
        JSON_EXTRACT: ['@preferences', 'language'] 
      }, 
      '=', 
      'en',
      'and',
      { 
        UPPER: '@status' 
      },
      '=',
      'ACTIVE'
    ]
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Customer>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("Filtered results with functions in WHERE:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleCustomers.map((customer) => [customer, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 5: CONCAT and LENGTH functions
console.log("\n=== Example 5: CONCAT and LENGTH functions ===")
{
  // Define a D2QL query using CONCAT and LENGTH
  const query: Query = {
    select: [
      '@id',
      { 
        full_info: { 
          CONCAT: ['Customer: ', '@name', ' (', '@email', ')'] 
        } 
      },
      { 
        name_length: { 
          LENGTH: '@name' 
        } 
      }
    ],
    from: 'customers',
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Customer>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("CONCAT and LENGTH results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleCustomers.map((customer) => [customer, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Main execution
if (require.main === module) {
  console.log("Running D2QL functions examples...")
} 