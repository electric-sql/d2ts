/**
 * D2QL Where Conditions Example
 * 
 * This example demonstrates the usage of WHERE clauses in D2QL for filtering data
 * with both simple and complex conditions.
 */

import { D2, MultiSet, output, v, Antichain } from '@electric-sql/d2ts'
import { Query, compileQuery } from '@electric-sql/d2ts/d2ql'
import { Message, MessageType } from '@electric-sql/d2ts'

// Sample product type for examples
type Product = {
  id: number
  name: string
  category: string
  price: number
  inStock: boolean
  tags: string[]
}

// Sample data for examples
const sampleProducts: Product[] = [
  { id: 1, name: 'Laptop', category: 'Electronics', price: 1200, inStock: true, tags: ['tech', 'portable'] },
  { id: 2, name: 'Smartphone', category: 'Electronics', price: 800, inStock: true, tags: ['tech', 'mobile'] },
  { id: 3, name: 'Headphones', category: 'Accessories', price: 150, inStock: false, tags: ['audio', 'tech'] },
  { id: 4, name: 'Desk Chair', category: 'Furniture', price: 250, inStock: true, tags: ['office', 'comfort'] },
  { id: 5, name: 'Keyboard', category: 'Electronics', price: 100, inStock: true, tags: ['tech', 'input'] },
  { id: 6, name: 'Monitor', category: 'Electronics', price: 350, inStock: false, tags: ['tech', 'display'] },
]

// Example 1: Simple WHERE condition
console.log("\n=== Example 1: Simple WHERE condition ===")
{
  // Define a D2QL query with a simple WHERE condition
  const query: Query = {
    select: ['@id', '@name', '@price'],
    from: 'products',
    where: ['@price', '>', 200]
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Product>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("Products priced over $200:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleProducts.map((product) => [product, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 2: Compound WHERE condition with AND
console.log("\n=== Example 2: Compound WHERE condition with AND ===")
{
  // Define a D2QL query with a compound WHERE condition using AND
  const query: Query = {
    select: ['@id', '@name', '@category', '@price'],
    from: 'products',
    where: ['@category', '=', 'Electronics', 'and', '@price', '<', 500]
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Product>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("Electronics under $500:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleProducts.map((product) => [product, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Example 3: Complex WHERE condition with AND/OR combinations
console.log("\n=== Example 3: Complex WHERE condition with AND/OR combinations ===")
{
  // Define a D2QL query with a complex WHERE condition using nested conditions
  const query: Query = {
    select: ['@id', '@name', '@category', '@price', '@inStock'],
    from: 'products',
    where: [
      ['@price', '>=', 300, 'and', '@inStock', '=', true],
      'or',
      ['@category', '=', 'Accessories']
    ]
  }

  // Create a D2 graph
  const graph = new D2({ initialFrontier: v([0, 0]) })
  const input = graph.newInput<Product>()
  const pipeline = compileQuery(query, { [query.from]: input })

  // Add an output handler
  pipeline.pipe(
    output((message: Message<any>) => {
      if (message.type === MessageType.DATA) {
        const results = message.data.collection.getInner().map(([data]) => data)
        console.log("Expensive in-stock items OR accessories:", results)
      }
    }),
  )

  // Finalize the graph
  graph.finalize()

  // Send data to the input
  input.sendData(
    v([1, 0]),
    new MultiSet(sampleProducts.map((product) => [product, 1])),
  )
  input.sendFrontier(new Antichain([v([1, 0])]))

  // Run the graph
  graph.run()
}

// Main execution
if (require.main === module) {
  console.log("Running D2QL where conditions examples...")
} 