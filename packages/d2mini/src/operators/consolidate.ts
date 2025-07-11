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

    // Create generator that yields all items from all messages then consolidates
    function* generateConsolidatedResults() {
      const lazyResults = new LazyMultiSet(function* () {
        for (const message of messages) {
          for (const item of message) {
            yield item
          }
        }
      }).consolidate()
      
      yield* lazyResults
    }

    // Peek to see if there are any results after consolidation
    const generator = generateConsolidatedResults()
    const firstResult = generator.next()
    
    if (!firstResult.done) {
      // We have at least one result, create lazy set that includes the first result and the rest
      const lazyResults = new LazyMultiSet(function* () {
        // Yield the first result we already got
        yield firstResult.value
        // Yield the rest of the results
        yield* generator
      })

      this.output.sendData(lazyResults)
    }
    // If no results after consolidation, don't send anything
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
