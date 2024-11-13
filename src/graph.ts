/**
 * The implementation of dataflow graph edge, node, and graph objects, used to run a dataflow program.
 */

import type Database from 'better-sqlite3'
import { MultiSet } from './multiset'
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
    const out = [...this.#queue].reverse()
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

  sendData(version: Version, collection: MultiSet<T>): void {
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
  protected pendingWork = false
  protected inputFrontiers: Antichain[]
  protected outputFrontier: Antichain

  constructor(
    public id: number,
    inputs: DifferenceStreamReader<T>[],
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    this.inputs = inputs
    this.output = output
    this.inputFrontiers = inputs.map(() => initialFrontier)
    this.outputFrontier = initialFrontier
  }

  abstract run(): void

  hasPendingWork(): boolean {
    if (this.pendingWork) return true
    return this.inputs.some((input) => !input.isEmpty())
  }

  frontiers(): [Antichain[], Antichain] {
    return [this.inputFrontiers, this.outputFrontier]
  }
}

/**
 * A convenience implementation of a dataflow operator that has a handle to one
 * incoming stream of data, and one handle to an outgoing stream of data.
 */
export abstract class UnaryOperator<T> extends Operator<T> {
  constructor(
    public id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    super(id, [inputA], output, initialFrontier)
  }

  inputMessages(): Message<T>[] {
    return this.inputs[0].drain()
  }

  inputFrontier(): Antichain {
    return this.inputFrontiers[0]
  }

  setInputFrontier(frontier: Antichain): void {
    this.inputFrontiers[0] = frontier
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
    initialFrontier: Antichain,
  ) {
    super(id, [inputA, inputB], output, initialFrontier)
  }

  inputAMessages(): Message<T>[] {
    return this.inputs[0].drain()
  }

  inputAFrontier(): Antichain {
    return this.inputFrontiers[0]
  }

  setInputAFrontier(frontier: Antichain): void {
    this.inputFrontiers[0] = frontier
  }

  inputBMessages(): Message<T>[] {
    return this.inputs[1].drain()
  }

  inputBFrontier(): Antichain {
    return this.inputFrontiers[1]
  }

  setInputBFrontier(frontier: Antichain): void {
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
export class Graph {
  streams: DifferenceStreamReader<any>[]
  operators: Operator<any>[]
  #db: Database.Database | undefined

  constructor(
    streams: DifferenceStreamReader<any>[],
    operators: Operator<any>[],
    db: Database.Database | undefined = undefined,
  ) {
    this.streams = streams
    this.operators = operators
    this.#db = db
  }

  get db(): Database.Database | undefined {
    return this.#db
  }

  #stepInner(): void {
    for (const op of this.operators) {
      op.run()
    }
  }

  step(): void {
    // When we use SQLite, we wrap the step in a transaction to ensure that
    // we never have a partially applied a query state to the database.
    if (this.#db) {
      this.#db.transaction(() => {
        this.#stepInner()
      })()
    } else {
      this.#stepInner()
    }
  }
}
