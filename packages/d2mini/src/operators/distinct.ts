import { IStreamBuilder } from '../types.js'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
} from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { hash } from '../utils.js'
import { IMultiSet, LazyMultiSet } from '../multiset.js'

type HashedValue = string
type Multiplicity = number

/**
 * Operator that removes duplicates
 */
export class DistinctOperator<T> extends UnaryOperator<T> {
  #by: (value: T) => any
  #values: Map<HashedValue, Multiplicity> // keeps track of the number of times each value has been seen

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
    const updatedValues = new Map<HashedValue, [Multiplicity, T]>()

    // Compute the new multiplicity for each value
    for (const message of this.inputMessages()) {
      for (const [value, diff] of message) {
        const hashedValue = hash(this.#by(value))

        const oldMultiplicity =
          updatedValues.get(hashedValue)?.[0] ??
          this.#values.get(hashedValue) ??
          0
        const newMultiplicity = oldMultiplicity + diff

        updatedValues.set(hashedValue, [newMultiplicity, value])
      }
    }

    // Pre-compute state changes to determine what will be yielded
    const stateChanges = new Map<HashedValue, { oldMultiplicity: number; newMultiplicity: number; value: T }>()
    
    for (const [hashedValue, [newMultiplicity, value]] of updatedValues.entries()) {
      const oldMultiplicity = this.#values.get(hashedValue) ?? 0
      stateChanges.set(hashedValue, { oldMultiplicity, newMultiplicity, value })
    }

    // Update state immediately
    for (const [hashedValue, { newMultiplicity }] of stateChanges.entries()) {
      if (newMultiplicity === 0) {
        this.#values.delete(hashedValue)
      } else {
        this.#values.set(hashedValue, newMultiplicity)
      }
    }

    // Create lazy generator that yields results without intermediate array
    const lazyResults = new LazyMultiSet(function* () {
      for (const [, { oldMultiplicity, newMultiplicity, value }] of stateChanges.entries()) {
        if (oldMultiplicity <= 0 && newMultiplicity > 0) {
          // The value wasn't present in the stream
          // but with this change it is now present in the stream
          yield [value, 1]
        } else if (oldMultiplicity > 0 && newMultiplicity <= 0) {
          // The value was present in the stream
          // but with this change it is no longer present in the stream
          yield [value, -1]
        }
      }
    })

    this.output.sendData(lazyResults)
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
