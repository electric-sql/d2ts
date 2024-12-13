import { IStreamBuilder, PipedOperator } from '../types.js'
import { DifferenceStreamWriter } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { LinearUnaryOperator } from './base.js'
import { MultiSet } from '../multiset.js'

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
