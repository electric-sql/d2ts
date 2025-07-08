import { IStreamBuilder } from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { hash } from '../utils.js'
import { MultiSet } from '../multiset.js'

type HashedValue = string
type Multiplicity = number

/**
 * Operator that removes duplicates
 */
export class DistinctOperator<T> extends UnaryOperator<T> {
  #values: Map<HashedValue, Multiplicity> // keeps track of the number of times each value has been seen

  constructor(
    id: number,
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
  ) {
    super(id, input, output)
    this.#values = new Map()
  }

  run(): void {
    const updatedValues = new Map<HashedValue, [Multiplicity, T]>()

    // Compute the new multiplicity for each value
    for (const message of this.inputMessages()) {
      for (const [value, diff] of message.getInner()) {
        const hashedValue = hash(value)

        const oldMultiplicity = this.#values.get(hashedValue) ?? 0
        const newMultiplicity = oldMultiplicity + diff

        updatedValues.set(hashedValue, [newMultiplicity, value])
      }
    }

    const result: Array<[T, number]> = []

    // Check which values became visible or disappeared
    for (const [
      hashedValue,
      [newMultiplicity, value],
    ] of updatedValues.entries()) {
      const oldMultiplicity = this.#values.get(hashedValue) ?? 0

      if (newMultiplicity === 0) {
        this.#values.delete(hashedValue)
      } else {
        this.#values.set(hashedValue, newMultiplicity)
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
export function distinct<T>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter<T>(),
    )
    const operator = new DistinctOperator<T>(
      stream.graph.getNextOperatorId(),
      stream.connectReader() as DifferenceStreamReader<T>,
      output.writer,
    )
    stream.graph.addOperator(operator)
    stream.graph.addStream(output.connectReader())
    return output
  }
}
