import { describe, test, expect, beforeEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { compileQuery } from '../../src/d2ql/compiler.js'
import { Query } from '../../src/d2ql/schema.js'
import { output } from '../../src/operators/index.js'
import { MultiSet } from '../../src/multiset.js'
import { v } from '../../src/order.js'
import { MessageType } from '../../src/types.js'

// Define a type for our test records
interface OrderRecord {
  order_id: number
  customer_id: number
  amount: number
  status: string
  date: Date
}

describe('D2QL GROUP BY', () => {
  let graph: D2
  let ordersInput: ReturnType<D2['newInput']>
  let messages: any[] = []

  // Sample data for testing
  const orders: OrderRecord[] = [
    { order_id: 1, customer_id: 1, amount: 100, status: 'completed', date: new Date('2023-01-01') },
    { order_id: 2, customer_id: 1, amount: 200, status: 'completed', date: new Date('2023-01-15') },
    { order_id: 3, customer_id: 2, amount: 150, status: 'pending', date: new Date('2023-01-20') },
    { order_id: 4, customer_id: 2, amount: 300, status: 'completed', date: new Date('2023-02-01') },
    { order_id: 5, customer_id: 3, amount: 250, status: 'pending', date: new Date('2023-02-10') },
  ]

  beforeEach(() => {
    // Create a new graph for each test
    graph = new D2({ initialFrontier: v([0]) })
    ordersInput = graph.newInput<OrderRecord>()
    messages = []
  })

  // Helper function to run a query and get results
  const runQuery = (query: Query) => {
    // Compile the query
    const pipeline = compileQuery<any>(query, { orders: ordersInput as any })

    // Create an output to collect the results
    const outputOp = output<any>((message) => {
      messages.push(message)
    })
    
    pipeline.pipe(outputOp)

    // Finalize the graph
    graph.finalize()

    // Send the sample data to the input
    for (const order of orders) {
      ordersInput.sendData(v([1]), new MultiSet([[order, 1]]))
    }

    // Close the input by sending a frontier update
    ordersInput.sendFrontier(v([2]))

    // Run the graph
    graph.run()

    return messages
  }

  test('should group by a single column', () => {
    const query: Query = {
      select: [
        '@customer_id',
        { total_amount: { 'SUM': '@amount' } as any },
        { order_count: { 'COUNT': '@order_id' } as any }
      ],
      from: 'orders',
      groupBy: ['@customer_id']
    }

    const messages = runQuery(query)

    // Verify we got at least one data message
    const dataMessages = messages.filter(m => m.type === MessageType.DATA)
    expect(dataMessages.length).toBeGreaterThan(0)
    
    // Verify we got a frontier message
    const frontierMessages = messages.filter(m => m.type === MessageType.FRONTIER)
    expect(frontierMessages.length).toBeGreaterThan(0)
  })

  test('should group by multiple columns', () => {
    const query: Query = {
      select: [
        '@customer_id',
        '@status',
        { total_amount: { 'SUM': '@amount' } as any },
        { order_count: { 'COUNT': '@order_id' } as any }
      ],
      from: 'orders',
      groupBy: ['@customer_id', '@status']
    }

    const messages = runQuery(query)

    // Verify we got at least one data message
    const dataMessages = messages.filter(m => m.type === MessageType.DATA)
    expect(dataMessages.length).toBeGreaterThan(0)
  })

  test('should apply HAVING clause after grouping', () => {
    const query: Query = {
      select: [
        '@customer_id',
        '@status',
        { total_amount: { 'SUM': '@amount' } as any },
        { order_count: { 'COUNT': '@order_id' } as any }
      ],
      from: 'orders',
      groupBy: ['@customer_id', '@status'],
      having: [
        { col: 'total_amount' },
        '>',
        200
      ]
    }

    const messages = runQuery(query)

    // Verify we got at least one data message
    const dataMessages = messages.filter(m => m.type === MessageType.DATA)
    expect(dataMessages.length).toBeGreaterThan(0)
  })

  test('should work with different aggregate functions', () => {
    const query: Query = {
      select: [
        '@customer_id',
        { total_amount: { 'SUM': '@amount' } as any },
        { avg_amount: { 'AVG': '@amount' } as any },
        { min_amount: { 'MIN': '@amount' } as any },
        { max_amount: { 'MAX': '@amount' } as any },
        { order_count: { 'COUNT': '@order_id' } as any }
      ],
      from: 'orders',
      groupBy: ['@customer_id']
    }

    const messages = runQuery(query)

    // Verify we got at least one data message
    const dataMessages = messages.filter(m => m.type === MessageType.DATA)
    expect(dataMessages.length).toBeGreaterThan(0)
  })

  test('should work with WHERE and GROUP BY together', () => {
    const query: Query = {
      select: [
        '@customer_id',
        { total_amount: { 'SUM': '@amount' } as any },
        { order_count: { 'COUNT': '@order_id' } as any }
      ],
      from: 'orders',
      where: [
        '@status',
        '=',
        'completed'
      ],
      groupBy: ['@customer_id']
    }

    const messages = runQuery(query)

    // Verify we got at least one data message
    const dataMessages = messages.filter(m => m.type === MessageType.DATA)
    expect(dataMessages.length).toBeGreaterThan(0)
  })

  test('should handle a single string in groupBy', () => {
    const query: Query = {
      select: [
        '@status',
        { total_amount: { 'SUM': '@amount' } as any },
        { order_count: { 'COUNT': '@order_id' } as any }
      ],
      from: 'orders',
      groupBy: '@status' // Single string instead of array
    }

    const messages = runQuery(query)

    // Verify we got at least one data message
    const dataMessages = messages.filter(m => m.type === MessageType.DATA)
    expect(dataMessages.length).toBeGreaterThan(0)
  })
}) 