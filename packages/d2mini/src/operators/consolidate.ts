import { IStreamBuilder, PipedOperator } from '../types.js'
import { DifferenceStreamWriter, UnaryOperator } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { LazyMultiSet } from '../multiset.js'

/**
 * Operator that consolidates collections
 */
export class ConsolidateOperator<T> extends UnaryOperator<T> {
  run(): void {
    const messages = this.inputMessages()

    // Create lazy generator that yields all items from all messages
    const lazyResults = new LazyMultiSet(function* () {
      for (const message of messages) {
        for (const item of message) {
          yield item
        }
      }
    }).consolidate()

    // Only send if there are results after consolidation
    if (lazyResults.getInner().length > 0) {
      this.output.sendData(lazyResults)
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
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
