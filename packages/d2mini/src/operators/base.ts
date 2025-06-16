import { MultiSet } from '../multiset.js'
import { UnaryOperator } from '../graph.js'

/**
 * Base class for operators that process a single input stream
 */
export abstract class LinearUnaryOperator<T, U> extends UnaryOperator<T | U> {
  abstract inner(collection: MultiSet<T | U>): MultiSet<U>

  run(): void {
    for (const message of this.inputMessages()) {
      this.output.sendData(this.inner(message))
    }
  }
}
