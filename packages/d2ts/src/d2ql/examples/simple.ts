import { D2 } from '../../d2.js'
import { MultiSet } from '../../multiset.js'
import { v } from '../../order.js'
import { map } from '../../operators/map.js'
import { outputElectricMessages } from '../../electric/index.js'
// Import from the d2ql module (in an actual package this would be '@electric-sql/d2ts/d2ql')
import { Query } from '../schema.js'
import { createPipeline } from '../compiler.js'

// Define a simple query that selects specific columns and filters rows
const query: Query = {
  select: ['@id', '@name', { age_in_years: '@age' }],
  from: 'users',
  where: ['@age', '>', 21],
}

// Create a new D2 graph
const graph = new D2({ initialFrontier: v([0, 0]) })

// Sample data
type User = {
  id: number
  name: string
  age: number
  email: string
  active: boolean
}

// Create a pipeline from the query
const [input, pipeline] = createPipeline<User>(graph, query)

// Add an output to see the results
const messages: any[] = []
pipeline.pipe(
  map((message) => [message.id, message]), // Key for the electric messages
  outputElectricMessages((message) => {
    messages.push(message)
  }),
)

// Finalize the graph
graph.finalize()

// Send some sample data
input.sendData(
  v([1, 0]),
  new MultiSet<User>([
    [
      {
        id: 1,
        name: 'Alice',
        age: 25,
        email: 'alice@example.com',
        active: true,
      },
      1,
    ],
    [
      { id: 2, name: 'Bob', age: 19, email: 'bob@example.com', active: true },
      1,
    ],
    [
      {
        id: 3,
        name: 'Charlie',
        age: 30,
        email: 'charlie@example.com',
        active: false,
      },
      1,
    ],
    [
      { id: 4, name: 'Dave', age: 22, email: 'dave@example.com', active: true },
      1,
    ],
  ]),
)

// Send frontier
input.sendFrontier(v([1, 0]))

// Run the graph
graph.run()

// Log the results
console.log('Query Results:')
messages.forEach((message) => {
  console.log(message)
})
