import { MultiSet, MultiSetArray, IMultiSet } from './multiset.js'
import {
  IOperator,
  IDifferenceStreamReader,
  IDifferenceStreamWriter,
} from './types.js'

/**
 * A read handle to a dataflow edge that receives data from a writer.
 */
export class DifferenceStreamReader<T> implements IDifferenceStreamReader<T> {
  #queue: IMultiSet<T>[]

  constructor(queue: IMultiSet<T>[]) {
    this.#queue = queue
  }

  drain(): IMultiSet<T>[] {
    const out = [...this.#queue].reverse()
    this.#queue.length = 0
    return out
  }

  isEmpty(): boolean {
    return this.#queue.length === 0
  }
}

/**
 * A write handle to a dataflow edge that is allowed to publish data.
 */
export class DifferenceStreamWriter<T> implements IDifferenceStreamWriter<T> {
  #queues: IMultiSet<T>[][] = []

  sendData(collection: IMultiSet<T> | MultiSetArray<T>): void {
    if (!(collection instanceof MultiSet) && !('getInner' in collection)) {
      collection = new MultiSet(collection)
    }

    for (const q of this.#queues) {
      q.unshift(collection as IMultiSet<T>)
    }
  }

  newReader(): DifferenceStreamReader<T> {
    const q: IMultiSet<T>[] = []
    this.#queues.push(q)
    return new DifferenceStreamReader(q)
  }
}

/**
 * A generic implementation of a dataflow operator (node) that has multiple incoming edges (read handles) and
 * one outgoing edge (write handle).
 */
export abstract class Operator<T> implements IOperator<T> {
  protected inputs: DifferenceStreamReader<T>[]
  protected output: DifferenceStreamWriter<T>

  constructor(
    public id: number,
    inputs: DifferenceStreamReader<T>[],
    output: DifferenceStreamWriter<T>,
  ) {
    this.inputs = inputs
    this.output = output
  }

  abstract run(): void

  hasPendingWork(): boolean {
    return this.inputs.some((input) => !input.isEmpty())
  }
}

/**
 * A convenience implementation of a dataflow operator that has a handle to one
 * incoming stream of data, and one handle to an outgoing stream of data.
 */
export abstract class UnaryOperator<Tin, Tout = Tin> extends Operator<
  Tin | Tout
> {
  constructor(
    public id: number,
    inputA: DifferenceStreamReader<Tin>,
    output: DifferenceStreamWriter<Tout>,
  ) {
    super(id, [inputA], output)
  }

  inputMessages(): IMultiSet<Tin>[] {
    return this.inputs[0].drain() as IMultiSet<Tin>[]
  }
}

/**
 * A convenience implementation of a dataflow operator that has a handle to two
 * incoming streams of data, and one handle to an outgoing stream of data.
 */
export abstract class BinaryOperator<T> extends Operator<T> {
  constructor(
    public id: number,
    inputA: DifferenceStreamReader<T>,
    inputB: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
  ) {
    super(id, [inputA, inputB], output)
  }

  inputAMessages(): IMultiSet<T>[] {
    return this.inputs[0].drain()
  }

  inputBMessages(): IMultiSet<T>[] {
    return this.inputs[1].drain()
  }
}

/**
 * Base class for operators that process a single input stream
 */
export abstract class LinearUnaryOperator<T, U> extends UnaryOperator<T | U> {
  abstract inner(collection: IMultiSet<T | U>): IMultiSet<U>

  run(): void {
    for (const message of this.inputMessages()) {
      this.output.sendData(this.inner(message))
    }
  }
}
