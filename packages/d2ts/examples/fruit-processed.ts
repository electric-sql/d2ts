import { MultiSet } from '../src/multiset'
import { D2 } from '../src/index.js'
import { map, filter, reduce, debug, consolidate, output } from '../src/operators/index.js'
import { v } from '../src/order.js'
import { MessageType } from '../src/types.js'

type FruitOrder = {
  name: string,
  quantity: number,
  shipping_id: string,
  status: 'packed' | 'shipped' | 'delivered'
}

const graph = new D2({ initialFrontier: v(0) })
const input = graph.newInput<FruitOrder>()

// Track quantities by status
const materializedStatus = new Map<string, number>();
const materializedProcessed = new Map<string, number>();

function showStatus() {
  const obj = Object.fromEntries(materializedStatus.entries())
  console.log('Counts by Status:')
  console.log(JSON.stringify(obj, null, 2))
}

function showProcessed() {
  const obj = Object.fromEntries(materializedProcessed.entries())
  console.log('Fruit Processed:')
  console.log(JSON.stringify(obj, null, 2))
}

const statusTotals = input.pipe(
  // debug('Raw Input'),
  map((order) => [`${order.name}-${order.status}`, order.quantity] as [string, number]),
  // debug('After Map'),
  reduce((values) => {
    // The reduce function receives an array of [quantity, diff] for each key
    // `diff` being the change in number of occurrences of the specific quantity
    // It is not aware of the key, just that everything it is receiving is for the same key
    // Here we want to sum the quantity for each key, so a sum of num * diff
    let count = 0
    for (const [num, diff] of values) {
      count += num * diff
    }
    return [[count, 1]]
  }),
  // debug('Status Totals'),
  consolidate(),
  output((msg) => {
    if (msg.type === MessageType.DATA) {
      const entries = msg.data.collection.getInner();
      // The entreis are:
      // key: {fruit-name}-{status}
      // count: number of items in that status
      // diff: 1 if adding a row, -1 if removing a row
      for (const [[key, count], diff] of entries) {
        if (diff > 0) {
          materializedStatus.set(key, count)
        } else if (diff < 0) {
          materializedStatus.delete(key)
        }
      }
    }
  })
)

// Track total processed quantities regardless of status
const processedTotals = input.pipe(
  // debug('Raw Input'),
  map((order) => [order.name, order.quantity] as [string, number]),
  // debug('After Map'),
  reduce((values) => {
    // Count the total number of each fruit processed
    let count = 0
    for (const [num, diff] of values) {
      count += num * diff
    }
    return [[count, 1]]
  }),
  // debug('Total Processed'),
  consolidate(),
  output((msg) => {
    if (msg.type === MessageType.DATA) {
      const entries = msg.data.collection.getInner();
      for (const [[key, count], diff] of entries) {
        if (diff > 0) {
          materializedProcessed.set(key, count)
        } else if (diff < 0) {
          materializedProcessed.delete(key)
        }
      }
    }
  })
)

graph.finalize()

console.log('--------------------------------')

// Initial packing of orders
console.log('Sending initial orders')
input.sendData(v(0), new MultiSet([
  [{
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'packed'
  }, 1],
  [{
    name: 'banana',
    quantity: 150,
    shipping_id: 'B001',
    status: 'packed'
  }, 1]
]))

input.sendFrontier(v(1)) // Send a frontier to set the new minimum version
graph.step() // Step the graph to process the data
// Show the materialized status and processed totals:
showStatus()
showProcessed()

console.log('--------------------------------')

// Ship 2 orders
console.log('Shipping 2 orders')
input.sendData(v(1), new MultiSet([
  // Remove from packed status
  [{
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'packed'
  }, -1],
  // Add to shipped status
  [{
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'shipped'
  }, 1],
  
  [{
    name: 'banana',
    quantity: 150,
    shipping_id: 'B001',
    status: 'packed'
  }, -1],
  [{
    name: 'banana',
    quantity: 150,
    shipping_id: 'B001',
    status: 'shipped'
  }, 1]
]))

input.sendFrontier(v(2))
graph.step()
showStatus()
showProcessed()

console.log('--------------------------------')

// One order arrives
console.log('One order arrives')
input.sendData(v(2), new MultiSet([
  // Remove from shipped status
  [{
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'shipped'
  }, -1],
  // Add to delivered status
  [{
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'delivered'
  }, 1]
]))

input.sendFrontier(v(3))
graph.step()
showStatus()
showProcessed()

console.log('--------------------------------')

/*
Output:
--------------------------------
Sending initial orders
Counts by Status:
{
  "apple-packed": 100,
  "banana-packed": 150
}
Fruit Processed:
{
  "apple": 100,
  "banana": 150
}
--------------------------------
Shipping 2 orders
Counts by Status:
{
  "apple-shipped": 100,
  "banana-shipped": 150
}
Fruit Processed:
{
  "apple": 100,
  "banana": 150
}
--------------------------------
One order arrives
Counts by Status:
{
  "banana-shipped": 150,
  "apple-delivered": 100
}
Fruit Processed:
{
  "apple": 100,
  "banana": 150
}
--------------------------------
*/