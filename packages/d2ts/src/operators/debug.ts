import { IStreamBuilder, PipedOperator, DataMessage, MessageType } from '../types.js'
import { DifferenceStreamReader, DifferenceStreamWriter, UnaryOperator } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { Antichain } from '../order.js'

/**
 * Operator that logs debug information about the stream
 */
export class DebugOperator<T> extends UnaryOperator<T> {
  #name: string
  #indent: boolean

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    name: string,
    initialFrontier: Antichain,
    indent: boolean = false,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#name = name
    this.#indent = indent
  }

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        // eslint-disable-next-line no-console
        console.log(
          `debug ${
            this.#name
          } data: version: ${version.toString()} collection: ${collection.toString(this.#indent)}`,
        )
        this.output.sendData(version, collection)
      } else if (message.type === MessageType.FRONTIER) {
        const frontier = message.data as Antichain
        if (!this.inputFrontier().lessEqual(frontier)) {
          throw new Error('Invalid frontier update')
        }
        this.setInputFrontier(frontier)
        // eslint-disable-next-line no-console
        console.log(
          `debug ${this.#name} notification: frontier ${frontier.toString()}`,
        )

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
 * Logs debug information about the stream using console.log
 * @param name - The name to prefix debug messages with
 * @param indent - Whether to indent the debug output
 */
export function debug<T>(
  name: string,
  indent: boolean = false,
): PipedOperator<T, T> {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new DebugOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader(),
      output.writer,
      name,
      stream.graph.frontier(),
      indent,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
} 