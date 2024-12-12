import {
  DataMessage,
  IStreamBuilder,
  Message,
  MessageType,
  PipedOperator,
  KeyValue,
} from './types.js'
import { MultiSet } from './multiset.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
  BinaryOperator,
} from './graph.js'
import { Index } from './version-index.js'
import { Version, Antichain } from './order.js'
import { DefaultMap } from './utils.js'
import { StreamBuilder } from './d2.js'

// Don't judge, this is the only way to type this function.
// rxjs has very similar code to type its pipe function
// https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/util/pipe.ts
// We go to 20 operators deep, because surly that's enough for anyone...
// A user can always split the pipe into multiple pipes to get around this.
export function pipe<T, O>(o1: PipedOperator<T, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, T18>, o18: PipedOperator<T18, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, T18>, o18: PipedOperator<T18, T19>, o19: PipedOperator<T19, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, T4>, o4: PipedOperator<T4, T5>, o5: PipedOperator<T5, T6>, o6: PipedOperator<T6, T7>, o7: PipedOperator<T7, T8>, o8: PipedOperator<T8, T9>, o9: PipedOperator<T9, T10>, o10: PipedOperator<T10, T11>, o11: PipedOperator<T11, T12>, o12: PipedOperator<T12, T13>, o13: PipedOperator<T13, T14>, o14: PipedOperator<T14, T15>, o15: PipedOperator<T15, T16>, o16: PipedOperator<T16, T17>, o17: PipedOperator<T17, T18>, o18: PipedOperator<T18, T19>, o19: PipedOperator<T19, T20>, o20: PipedOperator<T20, O>): PipedOperator<T, O>

/**
 * Creates a new stream by piping the input stream through a series of operators
 */
export function pipe<T>(...operators: PipedOperator<any, any>[]) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    return stream.pipe(...operators)
  }
}

/**
 * Base class for operators that process a single input stream
 */
abstract class LinearUnaryOperator<T, U> extends UnaryOperator<T | U> {
  abstract inner(collection: MultiSet<T>): MultiSet<U>

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.output.sendData(version, this.inner(collection))
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
      }
    }

    if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(this.outputFrontier)
    }
  }
}

/**
 * Operator that applies a function to each element in the input stream
 */
export class MapOperator<T, U> extends LinearUnaryOperator<T, U> {
  #f: (data: T) => U

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<U>,
    f: (data: T) => U,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#f = f
  }

  inner(collection: MultiSet<T>): MultiSet<U> {
    return collection.map(this.#f)
  }
}

/**
 * Applies a function to each element in the input stream
 * @param f - The function to apply to each element
 */
export function map<T, O>(f: (data: T) => O): PipedOperator<T, O> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<O> => {
    const output = new StreamBuilder<O>(
      stream.graph,
      new DifferenceStreamWriter<O>(),
    )
    const operator = new MapOperator<T, O>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      f,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that filters elements from the input stream
 */
export class FilterOperator<T> extends LinearUnaryOperator<T, T> {
  #f: (data: T) => boolean

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    f: (data: T) => boolean,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#f = f
  }

  inner(collection: MultiSet<T>): MultiSet<T> {
    return collection.filter(this.#f)
  }
}

/**
 * Filters elements from the input stream
 * @param f - The predicate to filter elements
 */
export function filter<T>(f: (data: T) => boolean): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new FilterOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      f,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that negates the multiplicities in the input stream
 */
export class NegateOperator<T> extends LinearUnaryOperator<T, T> {
  inner(collection: MultiSet<T>): MultiSet<T> {
    return collection.negate()
  }
}

/**
 * Negates the multiplicities in the input stream
 */
export function negate<T>(): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new NegateOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that concatenates two input streams
 */
export class ConcatOperator<T, T2> extends BinaryOperator<T | T2> {
  run(): void {
    for (const message of this.inputAMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputAFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputAFrontier(frontier)
      }
    }

    for (const message of this.inputBMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputBFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputBFrontier(frontier)
      }
    }

    const inputFrontier = this.inputAFrontier().meet(this.inputBFrontier())
    if (!this.outputFrontier.lessEqual(inputFrontier)) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(inputFrontier)) {
      this.outputFrontier = inputFrontier
      this.output.sendFrontier(this.outputFrontier)
    }
  }
}

/**
 * Concatenates two input streams
 * @param other - The other stream to concatenate
 */
export function concat<T, T2>(
  other: IStreamBuilder<T2>,
): PipedOperator<T, T | T2> {
  return (stream: IStreamBuilder<T | T2>): IStreamBuilder<T | T2> => {
    if (stream.graph !== other.graph) {
      throw new Error('Cannot concat streams from different graphs')
    }
    const output = new StreamBuilder<T | T2>(
      stream.graph,
      new DifferenceStreamWriter<T | T2>(),
    )
    const operator = new ConcatOperator<T, T2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      other.connectReader(),
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that logs debug information about the stream
 */
export class DebugOperator<T> extends UnaryOperator<T> {
  #name: string
  #indent: boolean

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    name: string,
    initialFrontier: Antichain,
    indent: boolean = false,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#name = name
    this.#indent = indent
  }

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        // eslint-disable-next-line no-console
        console.log(
          `debug ${
            this.#name
          } data: version: ${version.toString()} collection: ${collection.toString(this.#indent)}`,
        )
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
        // eslint-disable-next-line no-console
        console.log(
          `debug ${this.#name} notification: frontier ${frontier.toString()}`,
        )

        if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
          throw new Error('Invalid frontier state')
        }
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(this.outputFrontier)
        }
      }
    }
  }
}

/**
 * Logs debug information about the stream using console.log
 * @param name - The name to prefix debug messages with
 * @param indent - Whether to indent the debug output
 */
export function debug<T>(
  name: string,
  indent: boolean = false,
): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new DebugOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      name,
      stream.graph.frontier(),
      indent,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that outputs the messages in the stream
 */
export class OutputOperator<T> extends UnaryOperator<T> {
  #fn: (data: Message<T>) => void

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    fn: (data: Message<T>) => void,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#fn = fn
  }

  run(): void {
    for (const message of this.inputMessages()) {
      this.#fn(message)
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
        if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
          throw new Error('Invalid frontier state')
        }
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(this.outputFrontier)
        }
      }
    }
  }
}

/**
 * Outputs the messages in the stream
 * @param fn - The function to call with each message
 */
export function output<T>(fn: (data: Message<T>) => void): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new OutputOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      fn,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that consolidates collections at each version
 */
export class ConsolidateOperator<T> extends UnaryOperator<T> {
  #collections = new DefaultMap<Version, MultiSet<T>>(() => new MultiSet<T>())

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.#collections.update(version, (existing) => {
          existing.extend(collection)
          return existing
        })
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
      }
    }

    // Find versions that are complete (not covered by input frontier)
    const finishedVersions = Array.from(this.#collections.entries()).filter(
      ([version]) => !this.inputFrontier().lessEqualVersion(version),
    )

    // Process and remove finished versions
    for (const [version, collection] of finishedVersions) {
      const consolidated = collection.consolidate()
      this.#collections.delete(version)
      this.output.sendData(version, consolidated)
    }

    if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(this.outputFrontier)
    }
  }
}

/**
 * Consolidates the elements in the stream
 */
export function consolidate<T>(): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new ConsolidateOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that joins two input streams
 */
export class JoinOperator<K, V1, V2> extends BinaryOperator<
  [K, V1] | [K, V2] | [K, [V1, V2]]
> {
  #indexA = new Index<K, V1>()
  #indexB = new Index<K, V2>()

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    inputB: DifferenceStreamReader<[K, V2]>,
    output: DifferenceStreamWriter<[K, [V1, V2]]>,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, inputB, output, initialFrontier)
  }

  run(): void {
    const deltaA = new Index<K, V1>()
    const deltaB = new Index<K, V2>()

    // Process input A
    for (const message of this.inputAMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<[K, V1]>
        for (const [item, multiplicity] of collection.getInner()) {
          const [key, value] = item
          deltaA.addValue(key, version, [value, multiplicity])
        }
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputAFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputAFrontier(frontier)
      }
    }

    // Process input B
    for (const message of this.inputBMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<[K, V2]>
        for (const [item, multiplicity] of collection.getInner()) {
          const [key, value] = item
          deltaB.addValue(key, version, [value, multiplicity])
        }
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputBFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputBFrontier(frontier)
      }
    }

    // Process results
    const results = new Map<Version, MultiSet<[K, [V1, V2]]>>()

    // Join deltaA with existing indexB
    for (const [version, collection] of deltaA.join(this.#indexB)) {
      const existing = results.get(version) || new MultiSet<[K, [V1, V2]]>()
      existing.extend(collection)
      results.set(version, existing)
    }

    // Append deltaA to indexA
    this.#indexA.append(deltaA)

    // Join existing indexA with deltaB
    for (const [version, collection] of this.#indexA.join(deltaB)) {
      const existing = results.get(version) || new MultiSet<[K, [V1, V2]]>()
      existing.extend(collection)
      results.set(version, existing)
    }

    // Send results
    for (const [version, collection] of results) {
      this.output.sendData(version, collection)
    }

    // Append deltaB to indexB
    this.#indexB.append(deltaB)

    // Update frontiers
    const inputFrontier = this.inputAFrontier().meet(this.inputBFrontier())
    if (!this.outputFrontier.lessEqual(inputFrontier)) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(inputFrontier)) {
      this.outputFrontier = inputFrontier
      this.output.sendFrontier(this.outputFrontier)
      this.#indexA.compact(this.outputFrontier)
      this.#indexB.compact(this.outputFrontier)
    }
  }
}

/**
 * Joins two input streams
 * @param other - The other stream to join with
 */
export function join<
  K,
  V1 extends T extends KeyValue<infer _KT, infer VT> ? VT : never,
  V2,
  T,
>(
  other: IStreamBuilder<KeyValue<K, V2>>,
): PipedOperator<T, KeyValue<K, [V1, V2]>> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, [V1, V2]>> => {
    if (stream.graph !== other.graph) {
      throw new Error('Cannot join streams from different graphs')
    }
    const output = new StreamBuilder<KeyValue<K, [V1, V2]>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, [V1, V2]>>(),
    )
    const operator = new JoinOperator<K, V1, V2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      other.connectReader() as DifferenceStreamReader<KeyValue<K, V2>>,
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Base operator for reduction operations
 */
export class ReduceOperator<K, V1, V2> extends UnaryOperator<[K, V1 | V2]> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, V2>()
  #keysTodo = new Map<Version, Set<K>>()
  #f: (values: [V1, number][]) => [V2, number][]

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, V2]>,
    f: (values: [V1, number][]) => [V2, number][],
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#f = f
  }

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<[K, V1]>
        for (const [item, multiplicity] of collection.getInner()) {
          const [key, value] = item
          this.#index.addValue(key, version, [value, multiplicity])

          let todoSet = this.#keysTodo.get(version)
          if (!todoSet) {
            todoSet = new Set<K>()
            this.#keysTodo.set(version, todoSet)
          }
          todoSet.add(key)

          // Add key to all join versions
          for (const v2 of this.#index.versions(key)) {
            const joinVersion = version.join(v2)
            let joinTodoSet = this.#keysTodo.get(joinVersion)
            if (!joinTodoSet) {
              joinTodoSet = new Set<K>()
              this.#keysTodo.set(joinVersion, joinTodoSet)
            }
            joinTodoSet.add(key)
          }
        }
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
      }
    }

    // Find versions that are complete
    const finishedVersions = Array.from(this.#keysTodo.entries())
      .filter(([version]) => !this.inputFrontier().lessEqualVersion(version))
      .sort(([a], [b]) => {
        return a.lessEqual(b) ? -1 : 1
      })

    for (const [version, keys] of finishedVersions) {
      const result: [[K, V2], number][] = []

      for (const key of keys) {
        const curr = this.#index.reconstructAt(key, version)
        const currOut = this.#indexOut.reconstructAt(key, version)
        const out = this.#f(curr)

        // Calculate delta between current and previous output
        const delta = new Map<string, number>()
        const values = new Map<string, V2>()
        for (const [value, multiplicity] of out) {
          const valueKey = JSON.stringify(value)
          values.set(valueKey, value)
          delta.set(valueKey, (delta.get(valueKey) || 0) + multiplicity)
        }
        for (const [value, multiplicity] of currOut) {
          const valueKey = JSON.stringify(value)
          values.set(valueKey, value)
          delta.set(valueKey, (delta.get(valueKey) || 0) - multiplicity)
        }

        // Add non-zero deltas to result
        for (const [valueKey, multiplicity] of delta) {
          const value = values.get(valueKey)!
          if (multiplicity !== 0) {
            result.push([[key, value], multiplicity])
            this.#indexOut.addValue(key, version, [value, multiplicity])
          }
        }
      }

      if (result.length > 0) {
        this.output.sendData(version, new MultiSet(result))
      }
      this.#keysTodo.delete(version)
    }

    if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(this.outputFrontier)
      this.#index.compact(this.outputFrontier)
      this.#indexOut.compact(this.outputFrontier)
    }
  }
}

/**
 * Reduces the elements in the stream by key
 */
export function reduce<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V1 extends T extends KeyValue<K, infer V> ? V : never,
  R,
  T,
>(f: (values: [V1, number][]) => [R, number][]) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, R>> => {
    const output = new StreamBuilder<KeyValue<K, R>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, R>>(),
    )
    const operator = new ReduceOperator<K, V1, R>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
      output.writer,
      f,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that counts elements by key
 */
export class CountOperator<K, V> extends ReduceOperator<K, V, number> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, number]>,
    initialFrontier: Antichain,
  ) {
    const countInner = (vals: [V, number][]): [number, number][] => {
      let count = 0
      for (const [_, diff] of vals) {
        count += diff
      }
      return [[count, 1]]
    }

    super(id, inputA, output, countInner, initialFrontier)
  }
}

/**
 * Counts the number of elements by key
 */
export function count<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V extends T extends KeyValue<K, infer V> ? V : never,
  T,
>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, number>> => {
    const output = new StreamBuilder<KeyValue<K, number>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, number>>(),
    )
    const operator = new CountOperator<K, V>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that removes duplicates by key
 */
export class DistinctOperator<K, V> extends ReduceOperator<K, V, V> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, V]>,
    initialFrontier: Antichain,
  ) {
    const distinctInner = (vals: [V, number][]): [V, number][] => {
      const consolidated = new Map<string, number>()
      const values = new Map<string, V>()
      for (const [val, diff] of vals) {
        const key = JSON.stringify(val)
        consolidated.set(key, (consolidated.get(key) || 0) + diff)
        values.set(key, val)
      }
      return Array.from(consolidated.entries())
        .filter(([_, count]) => count > 0)
        .map(([key, _]) => [values.get(key) as V, 1])
    }

    super(id, inputA, output, distinctInner, initialFrontier)
  }
}

/**
 * Removes duplicates by key
 */
export function distinct<
  K extends T extends KeyValue<infer K, infer _V> ? K : never,
  V extends T extends KeyValue<K, infer V> ? V : never,
  T,
>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<KeyValue<K, V>> => {
    const output = new StreamBuilder<KeyValue<K, V>>(
      stream.graph,
      new DifferenceStreamWriter<KeyValue<K, V>>(),
    )
    const operator = new DistinctOperator<K, V>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Operator that moves data into a new iteration scope
 */
export class IngressOperator<T> extends UnaryOperator<T> {
  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        const newVersion = version.extend()
        this.output.sendData(newVersion, collection)
        this.output.sendData(newVersion.applyStep(1), collection.negate())
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        const newFrontier = frontier.extend()
        if (!this.inputFrontier().lessEqual(newFrontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(newFrontier)
      }
    }

    if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(this.outputFrontier)
    }
  }
}

/**
 * Operator that moves data out of an iteration scope
 */
export class EgressOperator<T> extends UnaryOperator<T> {
  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        const newVersion = version.truncate()
        this.output.sendData(newVersion, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        const newFrontier = frontier.truncate()
        if (!this.inputFrontier().lessEqual(newFrontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(newFrontier)
      }
    }

    if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(this.outputFrontier)
    }
  }
}

/**
 * Operator that handles feedback in iteration loops.
 * This operator is responsible for:
 * 1. Incrementing versions for feedback data
 * 2. Managing the iteration state
 * 3. Determining when iterations are complete
 */
export class FeedbackOperator<T> extends UnaryOperator<T> {
  // Map from top-level version -> set of messages where we have
  // sent some data at that version
  #inFlightData = new DefaultMap<Version, Set<Version>>(() => new Set())
  // Versions where a given top-level version has updated
  // its iteration without sending any data.
  #emptyVersions = new DefaultMap<Version, Set<Version>>(() => new Set())
  #step: number

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    step: number,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#step = step
  }

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        const newVersion = version.applyStep(this.#step)
        const truncated = newVersion.truncate()
        this.output.sendData(newVersion, collection)

        // Record that we sent data at this version
        this.#inFlightData.get(truncated).add(newVersion)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
      }
    }

    // Increment the current input frontier
    const incrementedInputFrontier = this.inputFrontier().applyStep(this.#step)
    // Grab all of the elements from the potential output frontier
    const elements = incrementedInputFrontier.elements
    // Partition every element from this potential output frontier into one of
    // two sets, either elements to keep, or elements to reject
    const candidateOutputFrontier: Version[] = []
    const rejected: Version[] = []

    for (const elem of elements) {
      const truncated = elem.truncate()
      const inFlightSet = this.#inFlightData.get(truncated)

      // Always keep a frontier element if there is are differences associated
      // with its top-level version that are still in flight
      if (inFlightSet.size > 0) {
        candidateOutputFrontier.push(elem)

        // Clean up versions that will be closed by this frontier element
        for (const version of inFlightSet) {
          if (version.lessThan(elem)) {
            inFlightSet.delete(version)
          }
        }
      } else {
        // This frontier element does not have any differences associated with its
        // top-level version that were not closed out by prior frontier updates

        // Remember that we observed an "empty" update for this top-level version
        const emptySet = this.#emptyVersions.get(truncated)
        emptySet.add(elem)

        // Don't do anything if we haven't observed at least three "empty" frontier
        // updates for this top-level time
        if (emptySet.size <= 3) {
          candidateOutputFrontier.push(elem)
        } else {
          this.#inFlightData.delete(truncated)
          this.#emptyVersions.delete(truncated)
          rejected.push(elem)
        }
      }
    }

    // Ensure that we can still send data at all other top-level
    // versions that were not rejected
    for (const r of rejected) {
      for (const inFlightSet of this.#inFlightData.values()) {
        for (const version of inFlightSet) {
          candidateOutputFrontier.push(r.join(version))
        }
      }
    }

    // Construct a minimal antichain from the set of candidate elements
    const outputFrontier = new Antichain(candidateOutputFrontier)

    if (!this.outputFrontier.lessEqual(outputFrontier)) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(outputFrontier)) {
      this.outputFrontier = outputFrontier
      this.output.sendFrontier(this.outputFrontier)
    }
  }
}

function ingress<T>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter(),
    )
    const operator = new IngressOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

function egress<T>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter(),
    )
    const operator = new EgressOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}

/**
 * Iterates over the stream
 */
export function iterate<T>(
  f: (stream: IStreamBuilder<T>) => IStreamBuilder<T>,
) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    // Enter scope
    const newFrontier = stream.graph.frontier().extend()
    stream.graph.pushFrontier(newFrontier)

    const feedbackStream = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter(),
    )
    const entered = stream.pipe(ingress(), concat(feedbackStream))
    const result = f(entered)
    const feedbackOperator = new FeedbackOperator<T>(
      stream.graph.getNextOperatorId(),
      result.connectReader() as DifferenceStreamReader<T>,
      1,
      feedbackStream.writer,
      stream.graph.frontier(),
    )
    stream.graph.addStream(feedbackStream.connectReader())
    stream.graph.addOperator(feedbackOperator)

    // Exit scope
    stream.graph.popFrontier()

    return result.pipe(egress())
  }
}
