import { map, reduce, consolidate } from '@electric-sql/d2ts'
import { Store } from '@electric-sql/d2ts/store'

type FruitOrder = {
  name: string
  quantity: number
  shipping_id: string
  status: 'packed' | 'shipped' | 'delivered'
}

const fruitOrders = new Store<string, FruitOrder>()

const { materializedStatus, materializedProcessed } = Store.queryAll(
  [fruitOrders],
  ([fruitStream]) => {
    const statusStream = fruitStream.pipe(
      // debug('Raw Input'),
      map(
        ([orderId, order]) =>
          [`${order.name}-${order.status}`, order.quantity] as [string, number],
      ),
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
    )
    const processedStream = fruitStream.pipe(
      // debug('Raw Input'),
      map(
        ([orderId, order]) => [order.name, order.quantity] as [string, number],
      ),
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
    )

    const materializedStatus = Store.materialize(statusStream)
    const materializedProcessed = Store.materialize(processedStream)
    return { materializedStatus, materializedProcessed }
  },
)

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

console.log('--------------------------------')

// Initial packing of orders
console.log('Sending initial orders')
fruitOrders.transaction((tx) => {
  tx.set('A001', {
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'packed',
  })
  tx.set('B001', {
    name: 'banana',
    quantity: 150,
    shipping_id: 'B001',
    status: 'packed',
  })
})

// Show the materialized status and processed totals:
showStatus()
showProcessed()

console.log('--------------------------------')

// Ship 2 orders
console.log('Shipping 2 orders')
fruitOrders.transaction((tx) => {
  tx.set('A001', {
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'shipped',
  })
  tx.set('B001', {
    name: 'banana',
    quantity: 150,
    shipping_id: 'B001',
    status: 'shipped',
  })
})

showStatus()
showProcessed()

console.log('--------------------------------')

// One order arrives
console.log('One order arrives')
fruitOrders.transaction((tx) => {
  tx.set('A001', {
    name: 'apple',
    quantity: 100,
    shipping_id: 'A001',
    status: 'delivered',
  })
})

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
