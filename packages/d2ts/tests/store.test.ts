import { describe, it, expect, beforeEach } from 'vitest'
import { Store } from '../src/store'
import type { ChangeSet } from '../src/store'

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
      store.addEventListener('change', ((
        e: CustomEvent<ChangeSet<string, number>>,
      ) => {
        changes.push(e.detail)
      }) as EventListener)

      store.set('c', 3)

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([
        {
          type: 'insert',
          key: 'c',
          value: 3,
        },
      ])
    })

    it('should emit update events', () => {
      const changes: ChangeSet<string, number>[] = []
      store.addEventListener('change', ((
        e: CustomEvent<ChangeSet<string, number>>,
      ) => {
        changes.push(e.detail)
      }) as EventListener)

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
    })

    it('should emit delete events', () => {
      const changes: ChangeSet<string, number>[] = []
      store.addEventListener('change', ((
        e: CustomEvent<ChangeSet<string, number>>,
      ) => {
        changes.push(e.detail)
      }) as EventListener)

      store.delete('a')

      expect(changes).toHaveLength(1)
      expect(changes[0]).toEqual([
        {
          type: 'delete',
          key: 'a',
          previousValue: 1,
        },
      ])
    })
  })

  describe('transactions', () => {
    it('should batch changes in transactions', () => {
      const changes: ChangeSet<string, number>[] = []
      store.addEventListener('change', ((
        e: CustomEvent<ChangeSet<string, number>>,
      ) => {
        changes.push(e.detail)
      }) as EventListener)

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
})
