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

  constructor(initial: Map<K, V>) {
    super()
    this.#inner = new Map()
    this.#inTransaction = true
    for (const [key, value] of initial) {
      this.set(key, value)
    }
    this.#inTransaction = false
    this.#emitChanges()
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
}
