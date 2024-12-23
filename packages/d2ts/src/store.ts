import { MessageType } from './types'

import { output } from './operators/output'
import { IStreamBuilder } from './types'
import { D2 } from './d2'
import { MultiSet, MultiSetArray } from './multiset'

export type ChangeInsert<K, V> = {
  type: 'insert'
  key: K
  value: V
}

export type ChangeDelete<K, V> = {
  type: 'delete'
  key: K
  previousValue: V | undefined
}

export type ChangeUpdate<K, V> = {
  type: 'update'
  key: K
  value: V
  previousValue: V | undefined
}

export type Change<K, V> =
  | ChangeInsert<K, V>
  | ChangeDelete<K, V>
  | ChangeUpdate<K, V>

export type ChangeSet<K, V> = Change<K, V>[]

export class Store<K, V> extends EventTarget {
  #inner: Map<K, V>
  #inTransaction: boolean = false
  #pendingChanges: ChangeSet<K, V> = []

  constructor(initial?: Map<K, V>) {
    super()
    this.#inner = new Map()
    if (initial) {
      this.#inTransaction = true
      for (const [key, value] of initial) {
        this.set(key, value)
      }
      this.#inTransaction = false
      this.#emitChanges()
    }
  }

  #emitChanges() {
    if (this.#pendingChanges.length > 0) {
      this.dispatchEvent(
        new CustomEvent('change', {
          detail: this.#pendingChanges,
        }),
      )
      this.#pendingChanges = []
    }
  }

  clear(): void {
    for (const key of this.#inner.keys()) {
      this.delete(key)
    }
  }

  delete(key: K): void {
    const previousValue = this.#inner.get(key)
    this.#inner.delete(key)
    this.#pendingChanges.push({
      type: 'delete',
      key,
      previousValue,
    })
    if (!this.#inTransaction) {
      this.#emitChanges()
    }
  }

  entries(): IterableIterator<[K, V]> {
    return this.#inner.entries()
  }

  forEach(
    callbackfn: (value: V, key: K, map: Map<K, V>) => void,
    thisArg?: unknown,
  ): void {
    this.#inner.forEach(callbackfn, thisArg)
  }

  get(key: K): V | undefined {
    return this.#inner.get(key)
  }

  entriesAsChanges(): ChangeSet<K, V> {
    return Array.from(this.#inner.entries()).map(([key, value]) => ({
      type: 'insert',
      key,
      value,
    }))
  }

  has(key: K): boolean {
    return this.#inner.has(key)
  }

  keys(): IterableIterator<K> {
    return this.#inner.keys()
  }

  set(key: K, value: V): void {
    const previousValue = this.#inner.get(key)
    this.#inner.set(key, value)
    if (previousValue) {
      this.#pendingChanges.push({
        type: 'update',
        key,
        value,
        previousValue,
      })
    } else {
      this.#pendingChanges.push({
        type: 'insert',
        key,
        value,
      })
    }
    if (!this.#inTransaction) {
      this.#emitChanges()
    }
  }

  transaction(fn: (store: Store<K, V>) => void): void {
    this.#inTransaction = true
    fn(this)
    this.#inTransaction = false
    this.#emitChanges()
  }

  update(key: K, fn: (value: V | undefined) => V): void {
    const previousValue = this.#inner.get(key)
    const value = fn(previousValue)
    this.set(key, value)
  }

  values(): IterableIterator<V> {
    return this.#inner.values()
  }

  [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.#inner[Symbol.iterator]()
  }

  get size(): number {
    return this.#inner.size
  }

  static materialize<K, V>(stream: IStreamBuilder<[K, V]>): Store<K, V> {
    const store = new Store<K, V>()
    stream.pipe(
      output((msg) => {
        if (msg.type === MessageType.DATA) {
          const collection = msg.data.collection
          store.transaction((tx) => {
            const changesByKey = new Map<
              K,
              { deletes: number; inserts: number; value: V }
            >()

            for (const [[key, value], multiplicity] of collection.getInner()) {
              let changes = changesByKey.get(key)
              if (!changes) {
                changes = { deletes: 0, inserts: 0, value: value }
                changesByKey.set(key, changes)
              }

              if (multiplicity < 0) {
                changes.deletes += Math.abs(multiplicity)
              } else if (multiplicity > 0) {
                changes.inserts += multiplicity
                changes.value = value
              }
            }

            for (const [key, changes] of changesByKey) {
              const { deletes, inserts, value } = changes
              if (inserts >= deletes) {
                tx.set(key, value)
              } else if (deletes > 0) {
                tx.delete(key)
              }
            }
          })
        }
      }),
    )
    return store
  }

  static queryAll<K extends unknown, V extends unknown, R>(
    stores: Store<K, V>[],
    fn: (streams: IStreamBuilder<[K, V]>[]) => R,
  ): R {
    let time = 0
    const graph = new D2({ initialFrontier: time })
    const inputs = stores.map(() => graph.newInput<[K, V]>())
    const ret = fn(inputs)
    graph.finalize()

    for (let i = 0; i < stores.length; i++) {
      const store = stores[i]
      const input = inputs[i]
      store.addEventListener('change', (event) => {
        const rawChanges = (event as CustomEvent).detail as ChangeSet<K, V>
        const changes: MultiSetArray<[K, V]> = []
        for (const change of rawChanges) {
          switch (change.type) {
            case 'insert':
              changes.push([[change.key, change.value], 1])
              break
            case 'delete':
              changes.push([[change.key, change.previousValue!], -1])
              break
            case 'update':
              changes.push([[change.key, change.value], 1])
              changes.push([[change.key, change.previousValue!], -1])
              break
          }
        }
        input.sendData(time, new MultiSet(changes))
        input.sendFrontier(++time)
        graph.step()
        time++
      })
    }

    // Send the initial data
    for (let i = 0; i < stores.length; i++) {
      const store = stores[i]
      const input = inputs[i]
      const rawChanges = store.entriesAsChanges()
      const changes: MultiSetArray<[K, V]> = []
      for (const change of rawChanges) {
        switch (change.type) {
          case 'insert':
            changes.push([[change.key, change.value], 1])
            break
          case 'delete':
            changes.push([[change.key, change.previousValue!], -1])
            break
          case 'update':
            changes.push([[change.key, change.value], 1])
            changes.push([[change.key, change.previousValue!], -1])
            break
        }
      }
      input.sendData(time, new MultiSet(changes))
      input.sendFrontier(++time)
      graph.step()
    }

    return ret
  }
}
