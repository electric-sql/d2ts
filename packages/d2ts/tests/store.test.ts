import { describe, it, expect, beforeEach } from 'vitest'
import { Store } from '../src/store'
import type { ChangeSet } from '../src/store'
import { map, reduce, concat } from '../src/operators'

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

  describe('queryAll', () => {
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

      const materialized = Store.queryAll([store1, store2], ([s1, s2]) => {
        return Store.materialize(s1.pipe(concat(s2)))
      })

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

      const materialized = Store.queryAll([store1, store2], ([s1, s2]) => {
        // Combine both streams into one store
        return Store.materialize(s1.pipe(concat(s2)))
      })

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

      const { byStatus, totals } = Store.queryAll([orders], ([orderStream]) => {
        // Count by status
        const byStatus = Store.materialize(
          orderStream.pipe(
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
          orderStream.pipe(
            map(
              ([_, order]) => [order.name, order.quantity] as [string, number],
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
      })

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
  })

  describe('query', () => {
    it('should allow querying a single store', () => {
      const store = new Store<string, number>(
        new Map([
          ['a', 1],
          ['b', 2],
        ]),
      )

      const materialized = store.query((stream) =>
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
