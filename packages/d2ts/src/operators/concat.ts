import { IStreamBuilder, PipedOperator } from '../types.js'
import { DifferenceStreamWriter } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { BinaryOperator } from '../graph.js'
import { DataMessage, MessageType } from '../types.js'
import { Antichain } from '../order.js'

/**
 * Operator that concatenates two input streams
 */
export class ConcatOperator<T, T2> extends BinaryOperator<T | T2> {
  run(): void {
    for (const message of this.inputAMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputAFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputAFrontier(frontier)
      }
    }

    for (const message of this.inputBMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputBFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputBFrontier(frontier)
      }
    }

    const inputFrontier = this.inputAFrontier().meet(this.inputBFrontier())
    if (!this.outputFrontier.lessEqual(inputFrontier)) {
      throw new Error('Invalid frontier state')
    }
    if (this.outputFrontier.lessThan(inputFrontier)) {
      this.outputFrontier = inputFrontier
      this.output.sendFrontier(this.outputFrontier)
    }
  }
}

/**
 * Concatenates two input streams
 * @param other - The other stream to concatenate
 */
export function concat<T, T2>(
  other: IStreamBuilder<T2>,
): PipedOperator<T, T | T2> {
  return (stream: IStreamBuilder<T | T2>): IStreamBuilder<T | T2> => {
    if (stream.graph !== other.graph) {
      throw new Error('Cannot concat streams from different graphs')
    }
    const output = new StreamBuilder<T | T2>(
      stream.graph,
      new DifferenceStreamWriter<T | T2>(),
    )
    const operator = new ConcatOperator<T, T2>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      other.connectReader(),
      output.writer,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
} 