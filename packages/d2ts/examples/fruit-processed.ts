import { MultiSet } from '../src/multiset'
import { D2 } from '../src/index.js'
import { map, filter, reduce, debug, consolidate, output } from '../src/operators.js'
import { v } from '../src/order.js'
import { MessageType } from '../src/types.js'

type FruitOrder = {
  name: string,
  quantity: number,
  shipping_id: string,
  status: 'packed' | 'shipped' | 'delivered'
}

const graph = new D2({ initialFrontier: v([0, 0]) })
const input = graph.newInput<FruitOrder>()

// Track quantities by status
const finalState = new Map<string, number>();

const statusTotals = input.pipe(
  debug('Raw Input'),
  map((order) => [`${order.name}-${order.status}`, order.quantity] as [string, number]),
  debug('After Map'),
  reduce((values: [string, number][]) => {
    // Group by key and sum all changes
    const totals = new Map<string, number>();
    for (const [key, diff] of values) {
      const current = totals.get(key) || 0;
      totals.set(key, current + diff);
    }
    // Convert back to array format and filter out zero quantities
    return Array.from(totals)
      .filter(([_, total]) => total !== 0)
      .map(([key, total]) => [key, total]);
  }),
  debug('Status Totals'),
  consolidate(),
  output((msg) => {
    if (msg.type === MessageType.DATA) {
      const entries = msg.data.collection.getInner();
      console.log('Status Totals:', Object.fromEntries(entries));

      // Update final state
      for (const [key, diff] of entries) {
        const current = finalState.get(key) || 0;
        finalState.set(key, current + diff);
      }
    }
  })
)

// Track total processed quantities regardless of status
const processedTotals = input.pipe(
  debug('Raw Input'),
  map((order) => [order.name, order.quantity] as [string, number]),
  debug('After Map'),
  reduce((values: [string, number][]) => {
    // Group by key and sum all changes
    const totals = new Map<string, number>();
    for (const [key, diff] of values) {
      const current = totals.get(key) || 0;
      totals.set(key, current + diff);
    }
    // Convert back to array format
    return Array.from(totals).map(([key, total]) => [key, total]);
  }),
  debug('Total Processed'),
  consolidate(),
  output((msg) => {
    if (msg.type === MessageType.DATA) {
      const entries = msg.data.collection.getInner();
      console.log('Final Processed Totals:', Object.fromEntries(entries));
    }
  })
)

// Initial packing of orders
input.sendData(v([0, 0]), new MultiSet([
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

// Ship 3 orders
input.sendData(v([0, 1]), new MultiSet([
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

// One order arrives
input.sendData(v([0, 2]), new MultiSet([
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

// Step for each version
input.sendFrontier(v([0, 0]))
input.sendFrontier(v([0, 1]))
input.sendFrontier(v([0, 2]))
input.sendFrontier(v([0, 3]))
graph.finalize()

// Now process all versions
graph.step() // Process version [0, 0]
graph.step() // Process version [0, 1]
graph.step() // Process version [0, 2]
graph.step() // Process version [0, 3]

// Show final state
console.log('\nFinal State:');
const finalStateMap = new Map();

// Consolidate the final state by fruit-status
for (const [key, value] of finalState.entries()) {
  const [fruitStatus, quantity] = key;
  const currentValue = finalStateMap.get(fruitStatus) || { quantity: parseInt(quantity as string), total: 0 };
  currentValue.total += value;
  finalStateMap.set(fruitStatus, currentValue);
}

// Filter out zero values and sort
const finalEntries = Array.from(finalStateMap)
  .filter(([_, data]) => data.total !== 0)
  .sort();

for (const [key, data] of finalEntries) {
  const [fruit, status] = key.split('-');
  console.log(`${fruit} (${status}): ${data.quantity * data.total}`);
}
