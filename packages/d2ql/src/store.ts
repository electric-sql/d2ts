import {
  D2,
  MessageType,
  IStreamBuilder,
  map,
  output,
  RootStreamBuilder,
  MultiSet,
  MultiSetArray,
} from '@electric-sql/d2ts'
import { compileQuery } from './compiler.js'
import { KeyedQuery } from './schema.js'

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

export class Store<K, V> {
  #inner: Map<K, V>
  #inTransaction: boolean = false
  #pendingChanges: ChangeSet<K, V> = []
  #subscribers: Set<(changes: ChangeSet<K, V>) => void> = new Set()

  constructor(initial?: Map<K, V>) {
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
      const changes = this.#pendingChanges
      this.#subscribers.forEach((subscriber) => subscriber(changes))
      this.#pendingChanges = []
    }
  }

  subscribe(callback: (changes: ChangeSet<K, V>) => void): () => void {
    this.#subscribers.add(callback)
    return () => {
      this.#subscribers.delete(callback)
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

  pipe<R>(fn: (stream: IStreamBuilder<[K, V]>) => R): [R, () => void] {
    return Store.pipeAll({ single: this as Store<K, V> }, ({ single }) =>
      fn(single as IStreamBuilder<[K, V]>),
    )
  }

  /**
   * Query this store using D2QL and return a materialized store with the results
   * @param query The D2QL query to execute
   * @returns A tuple containing the materialized store and an unsubscribe function
   */
  query<T extends Record<string, any>>(
    query: KeyedQuery,
  ): [Store<string, T>, () => void] {
    return this.pipe((stream) => {
      const inputs: Record<string, IStreamBuilder<Record<string, unknown>>> = {
        [query.from]: stream.pipe(
          map(([_key, data]: [string, Record<string, unknown>]) => data),
        ),
      }
      const result = compileQuery<IStreamBuilder<[string, T]>>(query, inputs)
      return Store.materialize(result)
    })
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

  static pipeAll<StoreMap extends Record<string, Store<any, any>>, R>(
    stores: StoreMap,
    fn: (streams: {
      [K in keyof StoreMap]: StoreMap[K] extends Store<infer KT, infer VT>
        ? IStreamBuilder<[KT, VT]>
        : never
    }) => R,
  ): [R, () => void] {
    let time = 0
    const graph = new D2({ initialFrontier: time })
    const inputs: Record<string, RootStreamBuilder<unknown>> = {}
    for (const name of Object.keys(stores)) {
      inputs[name] = graph.newInput<unknown>()
    }
    const ret = fn(
      inputs as unknown as {
        [K in keyof StoreMap]: StoreMap[K] extends Store<infer KT, infer VT>
          ? IStreamBuilder<[KT, VT]>
          : never
      },
    )
    graph.finalize()

    const unsubscribes: (() => void)[] = []

    function sendChanges<K, V>(
      input: RootStreamBuilder<unknown>,
      rawChanges: ChangeSet<K, V>,
    ): void {
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
      input.sendData(
        time,
        new MultiSet(changes as unknown as MultiSetArray<unknown>),
      )
    }

    function sendFrontiers() {
      for (const name of Object.keys(stores)) {
        const input = inputs[name]
        input.sendFrontier(time)
      }
    }

    for (const name of Object.keys(stores)) {
      const store = stores[name]
      const input = inputs[name]
      const unsubscribe = store.subscribe((rawChanges) => {
        sendChanges(input, rawChanges)
        time = time + 1
        sendFrontiers()
        graph.run()
      })
      unsubscribes.push(unsubscribe)
    }

    for (const name of Object.keys(stores)) {
      const store = stores[name]
      const input = inputs[name]
      const rawChanges = store.entriesAsChanges()
      sendChanges(input, rawChanges)
    }
    time = time + 1
    sendFrontiers()
    graph.run()

    const unsubscribe = () => {
      for (const unsubscribe of unsubscribes) {
        unsubscribe()
      }
    }

    return [ret, unsubscribe]
  }

  static transactionAll<K, V>(
    stores: Store<K, V>[],
    fn: (stores: Store<K, V>[]) => void,
  ): void {
    stores.forEach((store) => (store.#inTransaction = true))
    try {
      fn(stores)
    } finally {
      stores.forEach((store) => {
        store.#inTransaction = false
        store.#emitChanges()
      })
    }
  }

  /**
   * Query multiple stores using D2QL and return a materialized store with the results
   * @param stores A record mapping table names to stores
   * @param query The D2QL query to execute
   * @returns A tuple containing the materialized store and an unsubscribe function
   */
  static queryAll<
    StoreMap extends Record<string, Store<string, any>>,
    T extends Record<string, unknown>,
  >(stores: StoreMap, query: KeyedQuery): [Store<string, T>, () => void] {
    return Store.pipeAll(stores, (streams) => {
      const inputs: Record<string, IStreamBuilder<Record<string, unknown>>> = {}
      for (const [tableName, stream] of Object.entries(streams)) {
        inputs[tableName] = stream.pipe(
          map(([_key, data]: [string, Record<string, unknown>]) => data),
        )
      }
      const result = compileQuery<IStreamBuilder<[string, T]>>(query, inputs)
      return Store.materialize(result)
    })
  }
}
