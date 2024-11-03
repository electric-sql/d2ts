/**
 * The implementation of dataflow graph edge, node, and graph objects, used to run a dataflow program.
 */

import { MultiSetArray } from './multiset'
import { Version, Antichain } from './order'
import {
  Message,
  MessageType,
  IOperator,
  DataMessage,
  FrontierMessage,
} from './types'

/**
 * A read handle to a dataflow edge that receives data and frontier updates from a writer.
 *
 * The data received over this edge are pairs of (version, MultiSet) and the frontier
 * updates are either integers (in the one dimensional case) or Antichains (in the general
 * case).
 */
export class DifferenceStreamReader<T> {
  #queue: Message<T>[]

  constructor(queue: Message<T>[]) {
    this.#queue = queue
  }

  drain(): Message<T>[] {
    const out = [...this.#queue]
    this.#queue.length = 0
    return out
  }

  isEmpty(): boolean {
    return this.#queue.length === 0
  }

  probeFrontierLessThan(frontier: Antichain): boolean {
    for (const { type, data } of this.#queue) {
      if (type === MessageType.FRONTIER) {
        const receivedFrontier = data as FrontierMessage
        if (frontier.lessEqual(receivedFrontier as Antichain)) {
          return false
        }
      }
    }
    return true
  }
}

/**
 * A write handle to a dataflow edge that is allowed to publish data and send
 * frontier updates.
 */
export class DifferenceStreamWriter<T> {
  #queues: Message<T>[][] = []
  frontier: Antichain | null = null

  sendData(version: Version, collection: MultiSetArray<T>): void {
    if (this.frontier) {
      if (!this.frontier.lessEqualVersion(version)) {
        throw new Error('Invalid version')
      }
    }

    const dataMessage: DataMessage<T> = { version, collection }
    for (const q of this.#queues) {
      q.unshift({
        type: MessageType.DATA,
        data: dataMessage,
      })
    }
  }

  sendFrontier(frontier: Antichain): void {
    if (this.frontier && !this.frontier.lessEqual(frontier)) {
      throw new Error('Invalid frontier')
    }

    this.frontier = frontier
    for (const q of this.#queues) {
      q.unshift({ type: MessageType.FRONTIER, data: frontier })
    }
  }

  newReader(): DifferenceStreamReader<T> {
    const q: Message<T>[] = []
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
  protected f: () => void
  protected pendingWork = false
  protected inputFrontiers: Version[]
  protected outputFrontier: Version

  constructor(
    inputs: DifferenceStreamReader<T>[],
    output: DifferenceStreamWriter<T>,
    f: () => void,
    initialFrontier: Version,
  ) {
    this.inputs = inputs
    this.output = output
    this.f = f
    this.inputFrontiers = inputs.map(() => initialFrontier)
    this.outputFrontier = initialFrontier
  }

  run(): void {
    this.f()
  }

  hasPendingWork(): boolean {
    if (this.pendingWork) return true
    return this.inputs.some((input) => !input.isEmpty())
  }

  frontiers(): [Version[], Version] {
    return [this.inputFrontiers, this.outputFrontier]
  }
}

/**
 * A convenience implementation of a dataflow operator that has a handle to one
 * incoming stream of data, and one handle to an outgoing stream of data.
 */
export class UnaryOperator<T> extends Operator<T> {
  constructor(
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    f: () => void,
    initialFrontier: Version,
  ) {
    super([inputA], output, f, initialFrontier)
  }

  inputMessages(): Message<T>[] {
    return this.inputs[0].drain()
  }

  inputFrontier(): Version {
    return this.inputFrontiers[0]
  }

  setInputFrontier(frontier: Version): void {
    this.inputFrontiers[0] = frontier
  }
}

/**
 * A convenience implementation of a dataflow operator that has a handle to two
 * incoming streams of data, and one handle to an outgoing stream of data.
 */
export class BinaryOperator<T> extends Operator<T> {
  constructor(
    inputA: DifferenceStreamReader<T>,
    inputB: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    f: () => void,
    initialFrontier: Version,
  ) {
    super([inputA, inputB], output, f, initialFrontier)
  }

  inputAMessages(): Message<T>[] {
    return this.inputs[0].drain()
  }

  inputAFrontier(): Version {
    return this.inputFrontiers[0]
  }

  setInputAFrontier(frontier: Version): void {
    this.inputFrontiers[0] = frontier
  }

  inputBMessages(): Message<T>[] {
    return this.inputs[1].drain()
  }

  inputBFrontier(): Version {
    return this.inputFrontiers[1]
  }

  setInputBFrontier(frontier: Version): void {
    this.inputFrontiers[1] = frontier
  }
}

/**
 * An implementation of a dataflow graph.
 *
 * This implementation needs to keep the entire set of nodes so that they
 * may be run, and only keeps a set of read handles to all edges for debugging
 * purposes. Calling this a graph instead of a 'bag of nodes' is misleading, because
 * this object does not actually know anything about the connections between the
 * various nodes.
 */
export class Graph<T> {
  streams: DifferenceStreamReader<T>[]
  operators: Operator<T>[]

  constructor(streams: DifferenceStreamReader<T>[], operators: Operator<T>[]) {
    this.streams = streams
    this.operators = operators
  }

  step(): void {
    for (const op of this.operators) {
      op.run()
    }
  }
}

export { MessageType }
