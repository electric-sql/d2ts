import { IStreamBuilder } from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { MultiSet } from '../multiset.js'
import { hash } from '../utils.js'

type Multiplicity = number

/**
 * Operator that removes duplicates
 */
export class DistinctOperator<T> extends UnaryOperator<T> {
  #by: (value: T) => any
  #values: Map<string, { multiplicity: Multiplicity, value: T }> // keeps track of the number of times each distinct value has been seen

  constructor(
    id: number,
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    by: (value: T) => any = (value: T) => value,
  ) {
    super(id, input, output)
    this.#by = by
    this.#values = new Map()
  }

  run(): void {
    const updatedValues = new Map<string, [Multiplicity, T]>()

    // Compute the new multiplicity for each value
    for (const message of this.inputMessages()) {
      for (const [value, diff] of message.getInner()) {
        const distinctValue = this.#by(value)
        const distinctKey = hash(distinctValue)

        const oldMultiplicity =
          updatedValues.get(distinctKey)?.[0] ??
          this.#values.get(distinctKey)?.multiplicity ??
          0
        const newMultiplicity = oldMultiplicity + diff

        updatedValues.set(distinctKey, [newMultiplicity, value])
      }
    }

    const result: Array<[T, number]> = []

    // Check which values became visible or disappeared
    for (const [
      distinctKey,
      [newMultiplicity, value],
    ] of updatedValues.entries()) {
      const oldMultiplicity = this.#values.get(distinctKey)?.multiplicity ?? 0

      if (newMultiplicity === 0) {
        this.#values.delete(distinctKey)
      } else {
        this.#values.set(distinctKey, { multiplicity: newMultiplicity, value })
      }

      if (oldMultiplicity <= 0 && newMultiplicity > 0) {
        // The value wasn't present in the stream
        // but with this change it is now present in the stream
        result.push([value, 1])
      } else if (oldMultiplicity > 0 && newMultiplicity <= 0) {
        // The value was present in the stream
        // but with this change it is no longer present in the stream
        result.push([value, -1])
      }
    }

    if (result.length > 0) {
      this.output.sendData(new MultiSet(result))
    }
  }
}

/**
 * Removes duplicate values
 */
export function distinct<T>(by: (value: T) => any = (value: T) => value) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new DistinctOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<T>,
      output.writer,
      by,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
