import { DataMessage, MessageType } from '../types.js'
import { MultiSet } from '../multiset.js'
import { UnaryOperator } from '../graph.js'
import { Antichain } from '../order.js'

/**
 * Base class for operators that process a single input stream
 */
export abstract class LinearUnaryOperator<T, U> extends UnaryOperator<T | U> {
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