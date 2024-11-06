import {
  BinaryOperator,
  DifferenceStreamReader,
  DifferenceStreamWriter,
  Graph,
  UnaryOperator,
} from './graph'
import { Antichain } from './order'
import { Message } from './types'
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
} from './operators'

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
  concat(other: DifferenceStreamBuilder<T>): DifferenceStreamBuilder<T> {
    if (this.#graph !== other.#graph) {
      throw new Error('Cannot concat streams from different graphs')
    }
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new ConcatOperator<T>(
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
  debug(name: string = ''): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph)
    const operator = new DebugOperator<T>(
      this.connectReader(),
      output.writer(),
      name,
      this.#graph.frontier(),
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
    const operator = new JoinOperator<K, V1, V2>(
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
   * Counts the number of elements by key
   */
  count<
    K extends T extends KeyValue<infer K, infer _V> ? K : never,
    V extends T extends KeyValue<K, infer V> ? V : never,
  >(): DifferenceStreamBuilder<KeyValue<K, number>> {
    const output = new DifferenceStreamBuilder<KeyValue<K, number>>(this.#graph)
    const operator = new CountOperator<K, V>(
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
    const operator = new DistinctOperator<K, V>(
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
    const operator = new ConsolidateOperator<T>(
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
    // TODO: I think the types are wrong here
    this.#startScope()
    const feedbackStream = new DifferenceStreamBuilder<T>(this.#graph)
    const entered = this.#ingress().concat(feedbackStream)
    const result = f(entered)
    const feedbackOperator = new FeedbackOperator<T>(
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

  constructor(initialFrontier: Antichain) {
    this.#frontierStack = [initialFrontier]
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
