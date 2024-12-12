import {
  BinaryOperator,
  DifferenceStreamReader,
  DifferenceStreamWriter,
  Graph,
  UnaryOperator,
} from './graph.js'
import { Antichain } from './order.js'
import { Message } from './types.js'
import {
  ConcatOperator,
  ConsolidateOperator,
  CountOperator,
  DebugOperator,
  DistinctOperator,
  EgressOperator,
  FeedbackOperator,
  FilterOperator,
  IngressOperator,
  JoinOperator,
  MapOperator,
  NegateOperator,
  OutputOperator,
  ReduceOperator,
} from './operators.js'
import {
  ConsolidateOperatorSQLite,
  JoinOperatorSQLite,
  ReduceOperatorSQLite,
  CountOperatorSQLite,
  DistinctOperatorSQLite,
} from './operators-sqlite.js'
import type Database from 'better-sqlite3'

type KeyValue<K, V> = [K, V]

/**
 * A representation of a dataflow edge as the dataflow graph is being built.
 *
 * This object is only used to set up the dataflow graph, and does not actually
 * interact with any data. Manually creating an instance of this object is highly
 * unexpected - instead more normal usage would be to create an instance using
 * the new_input method on GraphBuilder.
 */
export class DifferenceStreamBuilder<T> {
  #writer: DifferenceStreamWriter<T>
  #graph: GraphBuilder

  constructor(graph: GraphBuilder) {
    this.#writer = new DifferenceStreamWriter<T>()
    this.#graph = graph
  }

  connectReader(): DifferenceStreamReader<T> {
    return this.#writer.newReader()
  }

  writer(): DifferenceStreamWriter<T> {
    return this.#writer
  }

  /**
   * Applies a function to each element in the stream
   */
  map<U>(f: (data: T) => U): DifferenceStreamBuilder<U> {
    const output = new DifferenceStreamBuilder<U>(this.#graph)
    const operator = new MapOperator<T, U>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      output.writer(),
      f,
      this.#graph.frontier(),
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Filters the elements in the stream
   */
  filter(f: (data: T) => boolean): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new FilterOperator<T>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      output.writer(),
      f,
      this.#graph.frontier(),
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Negates the multiplicities in the stream
   */
  negate(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new NegateOperator<T>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      output.writer(),
      this.#graph.frontier(),
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Concatenates two streams
   */
  concat<T2>(
    other: DifferenceStreamBuilder<T2>,
  ): DifferenceStreamBuilder<T | T2> {
    if (this.#graph !== other.#graph) {
      throw new Error('Cannot concat streams from different graphs')
    }
    const output = new DifferenceStreamBuilder<T | T2>(this.#graph)
    const operator = new ConcatOperator<T, T2>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      other.connectReader(),
      output.writer(),
      this.#graph.frontier(),
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Prints the data in the stream
   */
  debug(
    name: string = '',
    indent: boolean = false,
  ): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new DebugOperator<T>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      output.writer(),
      name,
      this.#graph.frontier(),
      indent,
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Outputs the messages in the stream
   */
  output(fn: (data: Message<T>) => void): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new OutputOperator<T>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      output.writer(),
      fn,
      this.#graph.frontier(),
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Joins two streams
   */
  join<K, V1 extends T extends KeyValue<K, infer V> ? V : never, V2>(
    other: DifferenceStreamBuilder<KeyValue<K, V2>>,
  ): DifferenceStreamBuilder<KeyValue<K, [V1, V2]>> {
    if (this.#graph !== other.#graph) {
      throw new Error('Cannot join streams from different graphs')
    }
    const output = new DifferenceStreamBuilder<KeyValue<K, [V1, V2]>>(
      this.#graph,
    )
    const operator = this.#graph.db
      ? new JoinOperatorSQLite<K, V1, V2>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
          other.connectReader(),
          output.writer(),
          this.#graph.frontier(),
          this.#graph.db,
        )
      : new JoinOperator<K, V1, V2>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
          other.connectReader(),
          output.writer(),
          this.#graph.frontier(),
        )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Reduces the elements in the stream by key
   */
  reduce<
    K extends T extends KeyValue<infer K, infer _V> ? K : never,
    V1 extends T extends KeyValue<K, infer V> ? V : never,
    R,
  >(
    f: (values: [V1, number][]) => [R, number][],
  ): DifferenceStreamBuilder<KeyValue<K, R>> {
    const output = new DifferenceStreamBuilder<KeyValue<K, R>>(this.#graph)
    const operator = this.#graph.db
      ? new ReduceOperatorSQLite<K, V1, R>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
          output.writer(),
          f,
          this.#graph.frontier(),
          this.#graph.db,
        )
      : new ReduceOperator<K, V1, R>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V1>>,
          output.writer(),
          f,
          this.#graph.frontier(),
        )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Counts the number of elements by key
   */
  count<
    K extends T extends KeyValue<infer K, infer _V> ? K : never,
    V extends T extends KeyValue<K, infer V> ? V : never,
  >(): DifferenceStreamBuilder<KeyValue<K, number>> {
    const output = new DifferenceStreamBuilder<KeyValue<K, number>>(this.#graph)
    const operator = this.#graph.db
      ? new CountOperatorSQLite<K, V>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
          output.writer(),
          this.#graph.frontier(),
          this.#graph.db,
        )
      : new CountOperator<K, V>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
          output.writer(),
          this.#graph.frontier(),
        )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Removes duplicates by key
   */
  distinct<
    K extends T extends KeyValue<infer K, infer _V> ? K : never,
    V extends T extends KeyValue<K, infer V> ? V : never,
  >(): DifferenceStreamBuilder<KeyValue<K, V>> {
    const output = new DifferenceStreamBuilder<KeyValue<K, V>>(this.#graph)
    const operator = this.#graph.db
      ? new DistinctOperatorSQLite<K, V>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
          output.writer(),
          this.#graph.frontier(),
          this.#graph.db,
        )
      : new DistinctOperator<K, V>(
          this.#graph.getNextOperatorId(),
          this.connectReader() as DifferenceStreamReader<KeyValue<K, V>>,
          output.writer(),
          this.#graph.frontier(),
        )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Consolidates the elements in the stream
   */
  consolidate(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = this.#graph.db
      ? new ConsolidateOperatorSQLite<T>(
          this.#graph.getNextOperatorId(),
          this.connectReader(),
          output.writer(),
          this.#graph.frontier(),
          this.#graph.db,
        )
      : new ConsolidateOperator<T>(
          this.#graph.getNextOperatorId(),
          this.connectReader(),
          output.writer(),
          this.#graph.frontier(),
        )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  #startScope(): void {
    const newFrontier = this.#graph.frontier().extend()
    this.#graph.pushFrontier(newFrontier)
  }

  #endScope(): void {
    this.#graph.popFrontier()
  }

  #ingress(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new IngressOperator<T>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      output.writer(),
      this.#graph.frontier(),
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  #egress(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new EgressOperator<T>(
      this.#graph.getNextOperatorId(),
      this.connectReader(),
      output.writer(),
      this.#graph.frontier(),
    )
    this.#graph.addOperator(operator)
    this.#graph.addStream(output.connectReader())
    return output
  }

  /**
   * Iterates over the stream
   */
  iterate(
    f: (stream: DifferenceStreamBuilder<T>) => DifferenceStreamBuilder<T>,
  ): DifferenceStreamBuilder<T> {
    this.#startScope()
    const feedbackStream = new DifferenceStreamBuilder<T>(this.#graph)
    const entered = this.#ingress().concat(feedbackStream)
    const result = f(entered)
    if (this.#graph.db) {
      throw new Error('iterate operator not (yet) implemented for SQLite')
    }
    const feedbackOperator = new FeedbackOperator<T>(
      this.#graph.getNextOperatorId(),
      result.connectReader(),
      1,
      feedbackStream.writer(),
      this.#graph.frontier(),
    )
    this.#graph.addStream(feedbackStream.connectReader())
    this.#graph.addOperator(feedbackOperator)
    this.#endScope()
    return result.#egress()
  }
}

/**
 * A representation of a dataflow graph as it is being built.
 */
export class GraphBuilder {
  #streams: DifferenceStreamReader<any>[] = []
  #operators: (UnaryOperator<any> | BinaryOperator<any>)[] = []
  #frontierStack: Antichain[] = []
  #db: Database.Database | undefined
  #nextOperatorId = 0

  constructor(
    initialFrontier: Antichain,
    db: Database.Database | undefined = undefined,
  ) {
    this.#frontierStack = [initialFrontier]
    this.#db = db
  }

  get db(): Database.Database | undefined {
    return this.#db
  }

  getNextOperatorId(): number {
    return this.#nextOperatorId++
  }

  newInput<T>(): [DifferenceStreamBuilder<T>, DifferenceStreamWriter<T>] {
    const streamBuilder = new DifferenceStreamBuilder<T>(this)
    this.#streams.push(streamBuilder.connectReader())
    return [streamBuilder, streamBuilder.writer()]
  }

  addOperator(operator: UnaryOperator<any> | BinaryOperator<any>): void {
    this.#operators.push(operator)
  }

  addStream(stream: DifferenceStreamReader<any>): void {
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
    return new Graph(this.#streams, this.#operators)
  }
}
