/**
 * D2QL Basic Select Example
 * 
 * This example demonstrates the basic usage of D2QL for selecting data
 * from a collection.
 */

import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts'
import { Query, compileQuery } from '@electric-sql/d2ql'
import { Message, MessageType } from '@electric-sql/d2ts'

// Sample user type for examples
type User = {
  id: number
  name: string
  age: number
  email: string
  active: boolean
}

// Sample data for examples
const sampleUsers: User[] = [
  { id: 1, name: 'Alice', age: 25, email: 'alice@example.com', active: true },
  { id: 2, name: 'Bob', age: 19, email: 'bob@example.com', active: true },
  { id: 3, name: 'Charlie', age: 30, email: 'charlie@example.com', active: false },
  { id: 4, name: 'Dave', age: 22, email: 'dave@example.com', active: true },
]

// Example 1: Select all columns
console.log("\n=== Example 1: Select all columns ===")
{
  // Define a D2QL query to select all columns
  const query: Query = {
    select: ['@id', '@name', '@age', '@email', '@active'],
    from: 'users',
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<User>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("Results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleUsers.map((user) => [user, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 2: Select specific columns with aliases
console.log("\n=== Example 2: Select specific columns with aliases ===")
{
  // Define a D2QL query to select specific columns with aliases
  const query: Query = {
    select: ['@id', { user_name: '@name' }, { years_old: '@age' }],
    from: 'users',
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<User>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("Results:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleUsers.map((user) => [user, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Main execution
if (require.main === module) {
  console.log("Running D2QL basic select examples...")
} 