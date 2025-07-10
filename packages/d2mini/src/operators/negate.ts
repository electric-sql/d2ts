import { IStreamBuilder, PipedOperator } from '../types.js'
import { DifferenceStreamReader, DifferenceStreamWriter } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { LinearUnaryOperator } from '../graph.js'
import { IMultiSet, LazyMultiSet } from '../multiset.js'

/**
 * Operator that negates all multiplicities in the input stream
 */
export class NegateOperator<T> extends LinearUnaryOperator<T, T> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
  ) {
    super(id, inputA, output)
  }

  inner(collection: IMultiSet<T>): IMultiSet<T> {
    // Use LazyMultiSet for lazy evaluation
    return LazyMultiSet.from(collection).negate()
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
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
