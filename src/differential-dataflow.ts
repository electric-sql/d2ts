/**
 * An implementation of differential dataflow.
 * 
 * Compared to the Rust implementation, this implementation is both much less performant
 * and more restrictive. Specifically, multiplicities in collections are constrained to
 * be integers, and versions (timestamps in the Rust codebase) are constrained to be
 * Version objects (integer tuples ordered by the product partial order).
 */

import { Antichain } from './order';
import { 
  Graph,
  DifferenceStreamReader,
  DifferenceStreamWriter,
  Operator
} from './graph';
import {
  MapOperator,
  FilterOperator,
  NegateOperator,
  ConcatOperator,
  DebugOperator,
  JoinOperator,
  CountOperator,
  ConsolidateOperator,
  DistinctOperator,
  FeedbackOperator,
  IngressOperator,
  EgressOperator,
  KeyedData
} from './operators';

/**
 * A representation of a dataflow edge as the dataflow graph is being built.
 * 
 * This object is only used to set up the dataflow graph, and does not actually
 * interact with any data. Manually creating an instance of this object is highly
 * unexpected - instead more normal usage would be to create an instance using
 * the new_input method on GraphBuilder.
 */
class DifferenceStreamBuilder<T> {
  #writer: DifferenceStreamWriter<T>;
  #graph: GraphBuilder;

  constructor(graph: GraphBuilder) {
    this.#writer = new DifferenceStreamWriter<T>();
    this.#graph = graph;
  }

  connectReader(): DifferenceStreamReader<T> {
    return this.#writer.newReader();
  }

  writer(): DifferenceStreamWriter<T> {
    return this.#writer;
  }

  map<U>(f: (data: T) => U): DifferenceStreamBuilder<U> {
    const output = new DifferenceStreamBuilder<U>(this.#graph);
    const operator = new MapOperator(
      this.connectReader(),
      output.writer(),
      f,
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  filter(f: (data: T) => boolean): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new FilterOperator(
      this.connectReader(),
      output.writer(),
      f,
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  negate(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new NegateOperator(
      this.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  concat(other: DifferenceStreamBuilder<T>): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new ConcatOperator(
      this.connectReader(),
      other.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  debug(name = ''): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new DebugOperator(
      this.connectReader(),
      output.writer(),
      name,
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  join<K, V1, V2>(this: DifferenceStreamBuilder<KeyedData<V1>>, other: DifferenceStreamBuilder<KeyedData<V2>>): DifferenceStreamBuilder<KeyedData<[V1, V2]>> {
    const output = new DifferenceStreamBuilder<KeyedData<[V1, V2]>>(this.#graph);
    const operator = new JoinOperator<K, V1, V2>(
      this.connectReader(),
      other.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  count(): DifferenceStreamBuilder<number> {
    const output = new DifferenceStreamBuilder<number>(this.#graph);
    const operator = new CountOperator(
      this.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  consolidate(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new ConsolidateOperator(
      this.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  distinct(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new DistinctOperator(
      this.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  iterate(f: (stream: DifferenceStreamBuilder<T>) => DifferenceStreamBuilder<T>): DifferenceStreamBuilder<T> {
    this.#startScope();
    const feedbackStream = new DifferenceStreamBuilder<T>(this.#graph);
    const entered = this.ingress().concat(feedbackStream);
    const result = f(entered);
    const feedbackOperator = new FeedbackOperator(
      result.connectReader(),
      1,
      feedbackStream.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addStream(feedbackStream.connectReader());
    this.#graph.addOperator(feedbackOperator);
    this.#endScope();
    return result.egress();
  }

  #startScope(): void {
    const newFrontier = this.#graph.frontier().extend();
    this.#graph.pushFrontier(newFrontier);
  }

  #endScope(): void {
    this.#graph.popFrontier();
  }

  ingress(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new IngressOperator(
      this.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }

  egress(): DifferenceStreamBuilder<T> {
    const output = new DifferenceStreamBuilder<T>(this.#graph);
    const operator = new EgressOperator(
      this.connectReader(),
      output.writer(),
      this.#graph.frontier().elements[0]
    );
    this.#graph.addOperator(operator);
    this.#graph.addStream(output.connectReader());
    return output;
  }
}

/**
 * A representation of a dataflow graph as it is being built.
 */
class GraphBuilder {
  #streams: DifferenceStreamReader<any>[] = [];
  #operators: Operator<any>[] = [];
  #frontierStack: Antichain[] = [];

  constructor(initialFrontier: Antichain) {
    this.#frontierStack = [initialFrontier];
  }

  /**
   * Creates a new input stream in the dataflow graph.
   * Returns a tuple of the stream builder and writer.
   */
  newInput<T>(): [DifferenceStreamBuilder<T>, DifferenceStreamWriter<T>] {
    const streamBuilder = new DifferenceStreamBuilder<T>(this);
    this.#streams.push(streamBuilder.connectReader());
    return [streamBuilder, streamBuilder.writer()];
  }

  addOperator<T>(operator: Operator<T>): void {
    this.#operators.push(operator);
  }

  addStream<T>(stream: DifferenceStreamReader<T>): void {
    this.#streams.push(stream);
  }

  frontier(): Antichain {
    return this.#frontierStack[this.#frontierStack.length - 1];
  }

  pushFrontier(newFrontier: Antichain): void {
    this.#frontierStack.push(newFrontier);
  }

  popFrontier(): void {
    this.#frontierStack.pop();
  }

  finalize<T>(): Graph<T> {
    return new Graph(this.#streams as DifferenceStreamReader<T>[], this.#operators as Operator<T>[]);
  }
}

export {
  DifferenceStreamBuilder,
  GraphBuilder
}; 