/**
 * D2QL Group By Example
 *
 * This example demonstrates the GROUP BY functionality in D2QL,
 * including aggregation functions and HAVING clauses.
 */

import { D2, MultiSet, output, v } from '@electric-sql/d2ts'
import { Query, compileQuery } from '@electric-sql/d2ts/d2ql'
import { Message, MessageType } from '@electric-sql/d2ts'

// Define a type for our orders
type Order = {
  order_id: number
  customer_id: number
  amount: number
  status: string
  date: Date
}

// Sample data
const orders: Order[] = [
  { order_id: 1, customer_id: 1, amount: 100, status: 'completed', date: new Date('2023-01-01') },
  { order_id: 2, customer_id: 1, amount: 200, status: 'completed', date: new Date('2023-01-15') },
  { order_id: 3, customer_id: 2, amount: 150, status: 'pending', date: new Date('2023-01-20') },
  { order_id: 4, customer_id: 2, amount: 300, status: 'completed', date: new Date('2023-02-01') },
  { order_id: 5, customer_id: 3, amount: 250, status: 'pending', date: new Date('2023-02-10') },
]

console.log('\n=== D2QL Group By Example ===')

// Define a query with groupBy
const query: Query = {
  select: [
    "@customer_id",
    "@status",
    { total_amount: { "SUM": "@amount" } as any },
    { order_count: { "COUNT": "@order_id" } as any },
    { avg_amount: { "AVG": "@amount" } as any }
  ],
  from: "orders",
  groupBy: ["@customer_id", "@status"],
  having: [
    { col: "total_amount" },
    ">",
    100
  ]
}

// Create a D2 graph
const graph = new D2({ initialFrontier: v([0]) })

// Create an input for orders
// Use any type to avoid type compatibility issues
const ordersInput = graph.newInput<any>()

// Compile the query
const pipeline = compileQuery(query, { orders: ordersInput })

// Add an output handler
let results: any[] = []
pipeline.pipe(
  output((message: Message<any>) => {
    if (message.type === MessageType.DATA) {
      // Extract the actual data from the message
      results = message.data.collection.getInner().map(([data]) => data)
    }
  }),
)

// Finalize the graph
graph.finalize()

// Send order data
for (const order of orders) {
  ordersInput.sendData(v([1]), new MultiSet([[order, 1]]))
}

// Close the input by sending a frontier update
ordersInput.sendFrontier(v([2]))

// Run the graph
graph.run()

// Display the results
console.log('Group By Example Results:')
console.log('------------------------')
console.log('Demonstrates:')
console.log('1. Grouping by multiple columns (@customer_id and @status)')
console.log('2. Aggregate functions (SUM, COUNT, AVG)')
console.log('3. HAVING clause to filter groups')
console.log('------------------------')
console.log(JSON.stringify(results, null, 2))

// Note about the example output
console.log('\nNote: In this standalone example, the aggregate functions are shown as')
console.log('function definitions rather than computed values. In a real application')
console.log('integrated with a database, these would be replaced with actual aggregated')
console.log('values (e.g., the sum of amounts, count of orders, etc.).') 