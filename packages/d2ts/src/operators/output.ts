import { IStreamBuilder, PipedOperator, Message, DataMessage, MessageType } from '../types.js'
import { DifferenceStreamReader, DifferenceStreamWriter, UnaryOperator } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { Antichain } from '../order.js'

/**
 * Operator that outputs the messages in the stream
 */
export class OutputOperator<T> extends UnaryOperator<T> {
  #fn: (data: Message<T>) => void

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    fn: (data: Message<T>) => void,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#fn = fn
  }

  run(): void {
    for (const message of this.inputMessages()) {
      this.#fn(message)
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
        if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
          throw new Error('Invalid frontier state')
        }
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(this.outputFrontier)
        }
      }
    }
  }
}

/**
 * Outputs the messages in the stream
 * @param fn - The function to call with each message
 */
export function output<T>(fn: (data: Message<T>) => void): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new OutputOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      fn,
      stream.graph.frontier(),
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
} 