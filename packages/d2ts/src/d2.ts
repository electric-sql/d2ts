import {
  BinaryOperator,
  DifferenceStreamWriter,
  UnaryOperator,
} from './graph.js'
import { DifferenceStreamReader } from './graph.js'
import { MultiSetArray, MultiSet } from './multiset.js'
import { Antichain, Version } from './order.js'
import { Store } from './store.js'
import {
  PipedOperator,
  IStreamBuilder,
  ID2,
  KeyValue,
  MessageType,
} from './types.js'
import { output } from './operators/output.js'

export type D2Options = {
  initialFrontier: Antichain | Version | number | number[]
}

export class D2 implements ID2 {
  #streams: DifferenceStreamReader<any>[] = []
  #operators: (UnaryOperator<any> | BinaryOperator<any>)[] = []
  #frontierStack: Antichain[] = []
  #nextOperatorId = 0
  #finalized = false

  constructor({ initialFrontier }: D2Options) {
    this.#frontierStack = [Antichain.create(initialFrontier)]
  }

  #checkNotFinalized(): void {
    if (this.#finalized) {
      throw new Error('Graph already finalized')
    }
  }

  getNextOperatorId(): number {
    this.#checkNotFinalized()
    return this.#nextOperatorId++
  }

  newInput<T>(): RootStreamBuilder<T> {
    this.#checkNotFinalized()
    const writer = new DifferenceStreamWriter<T>()
    // Use the root stream builder that exposes the sendData and sendFrontier methods
    const streamBuilder = new RootStreamBuilder<T>(this, writer)
    this.#streams.push(streamBuilder.connectReader())
    return streamBuilder
  }

  addOperator(operator: UnaryOperator<any> | BinaryOperator<any>): void {
    this.#checkNotFinalized()
    this.#operators.push(operator)
  }

  addStream(stream: DifferenceStreamReader<any>): void {
    this.#checkNotFinalized()
    this.#streams.push(stream)
  }

  frontier(): Antichain {
    return this.#frontierStack[this.#frontierStack.length - 1]
  }

  pushFrontier(newFrontier: Antichain): void {
    this.#frontierStack.push(newFrontier)
  }

  popFrontier(): void {
    this.#frontierStack.pop()
  }

  finalize() {
    this.#checkNotFinalized()
    this.#finalized = true
  }

  step(): void {
    if (!this.#finalized) {
      throw new Error('Graph not finalized')
    }
    for (const op of this.#operators) {
      op.run()
    }
  }

  pendingWork(): boolean {
    return this.#operators.some((op) => op.hasPendingWork())
  }

  run(): void {
    while (this.pendingWork()) {
      this.step()
    }
  }
}

export class StreamBuilder<T> implements IStreamBuilder<T> {
  #graph: ID2
  #writer: DifferenceStreamWriter<T>

  constructor(graph: ID2, writer: DifferenceStreamWriter<T>) {
    this.#graph = graph
    this.#writer = writer
  }

  connectReader(): DifferenceStreamReader<T> {
    return this.#writer.newReader()
  }

  get writer(): DifferenceStreamWriter<T> {
    return this.#writer
  }

  get graph(): ID2 {
    return this.#graph
  }

  // Don't judge, this is the only way to type this function.
  // rxjs has very similar code to type its pipe function
  // https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/util/pipe.ts
  // We go to 20 operators deep, because surly that's enough for anyone...
  // A user can always split the pipe into multiple pipes to get around this.
  pipe<O>(o1: PipedOperator<T, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, T18>, o18: PipedOperator<T18, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, T18>, o18: PipedOperator<T18, T19>, o19: PipedOperator<T19, O>): IStreamBuilder<O>
  // prettier-ignore
  pipe<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, T18>, o18: PipedOperator<T18, T19>, o19: PipedOperator<T19, T20>, o20: PipedOperator<T20, O>): IStreamBuilder<O>

  pipe(...operators: PipedOperator<any, any>[]): IStreamBuilder<any> {
    return operators.reduce((stream, operator) => {
      return operator(stream)
    }, this as IStreamBuilder<any>)
  }

  materialize<K, V>(this: StreamBuilder<KeyValue<K, V>>): Store<K, V> {
    const store = new Store<K, V>(new Map())
    this.pipe(
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
}

export class RootStreamBuilder<T> extends StreamBuilder<T> {
  sendData(
    version: Version | number | number[],
    collection: MultiSet<T> | MultiSetArray<T>,
  ): void {
    this.writer.sendData(version, collection)
  }

  sendFrontier(frontier: Antichain | Version | number | number[]): void {
    this.writer.sendFrontier(frontier)
  }
}
