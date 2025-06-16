import {
  IStreamBuilder,
  PipedOperator,
  DataMessage,
  MessageType,
} from '../types.js'
import { DifferenceStreamWriter, UnaryOperator } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { MultiSet } from '../multiset.js'
import { Antichain, Version } from '../order.js'
import { DefaultMap } from '../utils.js'

/**
 * Operator that buffers collections at each version
 * Ensured that completed versions are sent to the output as a whole, and in order
 */
export class BufferOperator<T> extends UnaryOperator<T> {
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
      this.#collections.delete(version)
      this.output.sendData(version, collection)
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
 * Buffers the elements in the stream
 * Ensured that completed versions are sent to the output as a whole, and in order
 */
export function buffer<T>(): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new BufferOperator<T>(
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
