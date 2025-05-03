import { describe, it, expect, beforeEach } from 'vitest'
import { Store } from '../src/store'
import type { ChangeSet } from '../src/store'
import { map, reduce, concat, join } from '@electric-sql/d2ts'
import { Query, KeyedQuery } from '../src/schema'

describe('Store', () => {
  let store: Store<string, number>

  beforeEach(() => {
    store = new Store(
      new Map([
        ['a', 1],
        ['b', 2],
      ]),
    )
  })

  it('should initialize with initial values', () => {
    expect(store.get('a')).toBe(1)
    expect(store.get('b')).toBe(2)
    expect(store.size).toBe(2)
  })

  describe('basic operations', () => {
    it('should set and get values', () => {
      store.set('c', 3)
      expect(store.get('c')).toBe(3)
    })

    it('should delete values', () => {
      store.delete('a')
      expect(store.get('a')).toBeUndefined()
      expect(store.size).toBe(1)
    })

    it('should check if key exists', () => {
      expect(store.has('a')).toBe(true)
      expect(store.has('z')).toBe(false)
    })

    it('should clear all values', () => {
      store.clear()
      expect(store.size).toBe(0)
    })
  })

  describe('iteration methods', () => {
    it('should iterate over entries', () => {
      const entries = Array.from(store.entries())
      expect(entries).toEqual([
        ['a', 1],
        ['b', 2],
      ])
    })

    it('should iterate over keys', () => {
      const keys = Array.from(store.keys())
      expect(keys).toEqual(['a', 'b'])
    })

    it('should iterate over values', () => {
      const values = Array.from(store.values())
      expect(values).toEqual([1, 2])
    })

    it('should support forEach', () => {
      const result: Array<[string, number]> = []
      store.forEach((value, key) => {
        result.push([key, value])
      })
      expect(result).toEqual([
        ['a', 1],
        ['b', 2],
      ])
    })
  })

  describe('change events', () => {
    it('should emit insert events', () => {
      const changes: ChangeSet<string, number>[] = []
      const unsubscribe = store.subscribe((change) => {
        changes.push(change)
      })

      store.set('c', 3)

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([
        {
          type: 'insert',
          key: 'c',
          value: 3,
        },
      ])

      unsubscribe()
    })

    it('should emit update events', () => {
      const changes: ChangeSet<string, number>[] = []
      const unsubscribe = store.subscribe((change) => {
        changes.push(change)
      })

      store.set('a', 10)

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([
        {
          type: 'update',
          key: 'a',
          value: 10,
          previousValue: 1,
        },
      ])

      unsubscribe()
    })

    it('should emit delete events', () => {
      const changes: ChangeSet<string, number>[] = []
      const unsubscribe = store.subscribe((change) => {
        changes.push(change)
      })

      store.delete('a')

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([
        {
          type: 'delete',
          key: 'a',
          previousValue: 1,
        },
      ])

      unsubscribe()
    })

    it('should stop receiving events after unsubscribe', () => {
      const changes: ChangeSet<string, number>[] = []
      const unsubscribe = store.subscribe((change) => {
        changes.push(change)
      })

      store.set('c', 3)
      unsubscribe()
      store.set('d', 4)

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([
        {
          type: 'insert',
          key: 'c',
          value: 3,
        },
      ])
    })
  })

  describe('transactions', () => {
    it('should batch changes in transactions', () => {
      const changes: ChangeSet<string, number>[] = []
      const unsubscribe = store.subscribe((change) => {
        changes.push(change)
      })

      store.transaction((store) => {
        store.set('c', 3)
        store.set('d', 4)
        store.delete('a')
      })

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([
        {
          type: 'insert',
          key: 'c',
          value: 3,
        },
        {
          type: 'insert',
          key: 'd',
          value: 4,
        },
        {
          type: 'delete',
          key: 'a',
          previousValue: 1,
        },
      ])

      unsubscribe()
    })
  })

  describe('update method', () => {
    it('should update existing values', () => {
      store.update('a', (value) => (value || 0) + 10)
      expect(store.get('a')).toBe(11)
    })

    it('should handle updates on non-existing keys', () => {
      store.update('z', (value) => (value || 0) + 5)
      expect(store.get('z')).toBe(5)
    })
  })

  describe('entriesAsChanges', () => {
    it('should return all entries as insert changes', () => {
      const changes = store.entriesAsChanges()
      expect(changes).toEqual([
        { type: 'insert', key: 'a', value: 1 },
        { type: 'insert', key: 'b', value: 2 },
      ])
    })
  })

  describe('Symbol.iterator', () => {
    it('should support for...of iteration', () => {
      const entries: Array<[string, number]> = []
      for (const entry of store) {
        entries.push(entry)
      }
      expect(entries).toEqual([
        ['a', 1],
        ['b', 2],
      ])
    })
  })

  describe('pipeAll', () => {
    it('should allow querying multiple stores', () => {
      const store1 = new Store<string, number>(
        new Map([
          ['a', 1],
          ['b', 2],
        ]),
      )
      const store2 = new Store<string, number>(
        new Map([
          ['c', 3],
          ['d', 4],
        ]),
      )

      const [materialized, _unsubscribe] = Store.pipeAll(
        { store1, store2 },
        ({ store1, store2 }) => {
          return Store.materialize(store1.pipe(concat(store2)))
        },
      )

      expect(Array.from(materialized.entries())).toEqual([
        ['a', 1],
        ['b', 2],
        ['c', 3],
        ['d', 4],
      ])
    })

    it('should react to changes in source stores', () => {
      const store1 = new Store<string, number>(new Map([['a', 1]]))
      const store2 = new Store<string, number>(new Map([['b', 2]]))

      const [materialized, _unsubscribe] = Store.pipeAll(
        { store1, store2 },
        ({ store1, store2 }) => {
          // Combine both streams into one store
          return Store.materialize(store1.pipe(concat(store2)))
        },
      )

      expect(Array.from(materialized.entries())).toEqual([
        ['a', 1],
        ['b', 2],
      ])

      // Make changes to source stores
      store1.set('a', 10)
      store2.set('c', 3)

      expect(Array.from(materialized.entries())).toEqual([
        ['a', 10],
        ['b', 2],
        ['c', 3],
      ])
    })

    it('should handle complex transformations', () => {
      type FruitOrder = {
        name: string
        quantity: number
        status: 'packed' | 'shipped'
      }

      const orders = new Store<string, FruitOrder>()

      const [{ byStatus, totals }, _unsubscribe] = Store.pipeAll(
        { orders },
        ({ orders }) => {
          // Count by status
          const byStatus = Store.materialize(
            orders.pipe(
              map(
                ([_, order]) =>
                  [`${order.name}-${order.status}`, order.quantity] as [
                    string,
                    number,
                  ],
              ),
            ),
          )

          // Count total by fruit
          const totals = Store.materialize(
            orders.pipe(
              map(
                ([_, order]) =>
                  [order.name, order.quantity] as [string, number],
              ),
              reduce((values) => {
                let sum = 0
                for (const [qty, diff] of values) {
                  sum += qty * diff
                }
                return [[sum, 1]]
              }),
            ),
          )

          return { byStatus, totals }
        },
      )

      // Add initial orders
      orders.transaction((tx) => {
        tx.set('1', { name: 'apple', quantity: 100, status: 'packed' })
        tx.set('2', { name: 'banana', quantity: 150, status: 'packed' })
      })

      expect(Array.from(byStatus.entries())).toEqual([
        ['apple-packed', 100],
        ['banana-packed', 150],
      ])

      expect(Array.from(totals.entries())).toEqual([
        ['apple', 100],
        ['banana', 150],
      ])

      // Update an order status
      orders.set('1', { name: 'apple', quantity: 100, status: 'shipped' })

      expect(Array.from(byStatus.entries())).toEqual([
        ['banana-packed', 150],
        ['apple-shipped', 100],
      ])

      // Totals shouldn't change since only status changed
      expect(Array.from(totals.entries())).toEqual([
        ['apple', 100],
        ['banana', 150],
      ])
    })

    it('should handle joins between stores', () => {
      // Create two stores with related data
      type Order = {
        id: string
        productId: string
        quantity: number
      }

      type Product = {
        id: string
        name: string
        price: number
      }

      const orders = new Store<string, Order>(
        new Map([
          ['order1', { id: 'order1', productId: 'prod1', quantity: 5 }],
          ['order2', { id: 'order2', productId: 'prod2', quantity: 3 }],
          ['order3', { id: 'order3', productId: 'prod1', quantity: 2 }],
        ]),
      )

      const products = new Store<string, Product>(
        new Map([
          ['prod1', { id: 'prod1', name: 'Apple', price: 1.2 }],
          ['prod2', { id: 'prod2', name: 'Banana', price: 0.8 }],
        ]),
      )

      // Use pipeAll to join the stores
      const [result, unsubscribe] = Store.pipeAll(
        { orders, products },
        ({ orders, products }) => {
          // Transform orders to have productId as the key
          const ordersWithProductIdKey = orders.pipe(
            map(([_, order]) => [order.productId, order] as [string, Order]),
          )

          // Transform products to have id as the key
          const productsWithIdKey = products.pipe(
            map(([_, product]) => [product.id, product] as [string, Product]),
          )

          // Join orders and products
          const joined = ordersWithProductIdKey.pipe(
            join(productsWithIdKey, 'inner'),
            map(([productId, [order, product]]) => {
              // In an inner join, both order and product will be non-null
              if (order && product) {
                return [
                  order.id,
                  {
                    id: order.id,
                    quantity: order.quantity,
                    productName: product.name,
                    price: product.price,
                  },
                ] as [
                  string,
                  {
                    id: string
                    quantity: number
                    productName: string
                    price: number
                  },
                ]
              }
              // This should never happen with inner join, but TypeScript needs it
              return ['', {}] as [
                string,
                {
                  id: string
                  quantity: number
                  productName: string
                  price: number
                },
              ]
            }),
          )

          return Store.materialize(joined)
        },
      )

      // Check that the join worked correctly
      expect(result.size).toBe(3)

      // Check specific values
      const order1 = result.get('order1')
      expect(order1).toBeDefined()
      expect(order1?.productName).toBe('Apple')
      expect(order1?.price).toBe(1.2)

      const order2 = result.get('order2')
      expect(order2).toBeDefined()
      expect(order2?.productName).toBe('Banana')
      expect(order2?.price).toBe(0.8)

      // Check that changes propagate
      orders.set('order4', { id: 'order4', productId: 'prod2', quantity: 7 })
      expect(result.size).toBe(4)

      const order4 = result.get('order4')
      expect(order4).toBeDefined()
      expect(order4?.productName).toBe('Banana')
      expect(order4?.price).toBe(0.8)

      // Clean up
      unsubscribe()
    })
  })

  describe('pipe', () => {
    it('should allow querying a single store', () => {
      const store = new Store<string, number>(
        new Map([
          ['a', 1],
          ['b', 2],
        ]),
      )

      const [materialized, _unsubscribe] = store.pipe((stream) =>
        Store.materialize(
          stream.pipe(
            map(([key, value]) => [key, value * 2] as [string, number]),
          ),
        ),
      )

      expect(Array.from(materialized.entries())).toEqual([
        ['a', 2],
        ['b', 4],
      ])

      store.set('c', 3)
      expect(Array.from(materialized.entries())).toEqual([
        ['a', 2],
        ['b', 4],
        ['c', 6],
      ])
    })
  })

  describe('query', () => {
    it('should allow querying a single store with D2QL', () => {
      type FruitOrder = {
        id: string
        name: string
        quantity: number
        status: 'packed' | 'shipped'
      }

      const store = new Store<string, FruitOrder>(
        new Map([
          [
            'order1',
            { id: 'order1', name: 'apple', quantity: 50, status: 'packed' },
          ],
          [
            'order2',
            { id: 'order2', name: 'banana', quantity: 30, status: 'shipped' },
          ],
          [
            'order3',
            { id: 'order3', name: 'apple', quantity: 20, status: 'packed' },
          ],
        ]),
      )

      const [result, unsubscribe] = store.query({
        select: ['@id', '@name', '@quantity'],
        from: 'orders',
        keyBy: '@id',
        where: ['@name', '=', 'apple'],
      })

      // Check that the result contains only apple orders
      expect(Array.from(result.entries()).length).toBe(2)
      expect(
        Array.from(result.entries()).every(
          ([_, value]) => value.name === 'apple',
        ),
      ).toBe(true)

      // Add a new apple order and check that it's reflected in the result
      store.set('order4', {
        id: 'order4',
        name: 'apple',
        quantity: 10,
        status: 'packed',
      })
      expect(Array.from(result.entries()).length).toBe(3)

      // Clean up
      unsubscribe()
    })
  })

  describe('queryAll', () => {
    it('should allow querying multiple stores with D2QL', () => {
      type Order = {
        productId: string
        quantity: number
      }

      type Product = {
        name: string
        price: number
      }

      const orders = new Store<string, Order>(
        new Map([
          ['order1', { id: 'order1', productId: 'prod1', quantity: 5 }],
          ['order2', { id: 'order2', productId: 'prod2', quantity: 3 }],
          ['order3', { id: 'order3', productId: 'prod1', quantity: 2 }],
        ]),
      )

      const products = new Store<string, Product>(
        new Map([
          ['prod1', { id: 'prod1', name: 'Apple', price: 1.2 }],
          ['prod2', { id: 'prod2', name: 'Banana', price: 0.8 }],
        ]),
      )

      const [result, unsubscribe] = Store.queryAll(
        { orders, products },
        {
          select: ['@orders.id', '@orders.quantity', '@products.price'],
          from: 'orders',
          join: [
            {
              type: 'inner',
              from: 'products',
              on: ['@orders.productId', '=', '@products.id'],
            },
          ],
          keyBy: '@id',
        },
      )

      // Check that the result contains all orders with their prices
      expect(Array.from(result.entries()).length).toBe(3)

      // Clean up
      unsubscribe()
    })
  })

  describe('transactionAll', () => {
    it('should batch changes across multiple stores', () => {
      const store1 = new Store<string, number>()
      const store2 = new Store<string, number>()

      const changes1: ChangeSet<string, number>[] = []
      const changes2: ChangeSet<string, number>[] = []

      const unsubscribe1 = store1.subscribe((change) => changes1.push(change))
      const unsubscribe2 = store2.subscribe((change) => changes2.push(change))

      Store.transactionAll([store1, store2], ([s1, s2]) => {
        s1.set('a', 1)
        s2.set('b', 2)
        s1.set('c', 3)
        s2.set('d', 4)
      })

      expect(changes1).toHaveLength(1)
      expect(changes1[0]).toEqual([
        { type: 'insert', key: 'a', value: 1 },
        { type: 'insert', key: 'c', value: 3 },
      ])

      expect(changes2).toHaveLength(1)
      expect(changes2[0]).toEqual([
        { type: 'insert', key: 'b', value: 2 },
        { type: 'insert', key: 'd', value: 4 },
      ])

      unsubscribe1()
      unsubscribe2()
    })

    it('should handle errors and still emit changes', () => {
      const store1 = new Store<string, number>()
      const store2 = new Store<string, number>()

      const changes: ChangeSet<string, number>[] = []
      const unsubscribe = store1.subscribe((change) => changes.push(change))

      expect(() => {
        Store.transactionAll([store1, store2], ([s1, _s2]) => {
          s1.set('a', 1)
          throw new Error('test error')
        })
      }).toThrow('test error')

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([{ type: 'insert', key: 'a', value: 1 }])

      unsubscribe()
    })
  })
})
