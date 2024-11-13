import { DataMessage, Message, MessageType } from './types'
import { MultiSet } from './multiset'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
  BinaryOperator,
} from './graph'
import { Index } from './version-index'
import { Version, Antichain } from './order'
import { DefaultMap } from './utils'

/**
 * Base class for operators that process a single input stream
 */
class LinearUnaryOperator<T, U> extends UnaryOperator<T | U> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<U>,
    f: (collection: MultiSet<T>) => MultiSet<U>,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<T>
          this.output.sendData(version, f(collection))
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

    super(id, inputA, output, inner, initialFrontier)
  }
}

/**
 * Operator that applies a function to each element in the input stream
 */
export class MapOperator<T, U> extends LinearUnaryOperator<T, U> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<U>,
    f: (data: T) => U,
    initialFrontier: Antichain,
  ) {
    const mapInner = (collection: MultiSet<T>) => collection.map(f)
    super(id, inputA, output, mapInner, initialFrontier)
  }
}

/**
 * Operator that filters elements from the input stream
 */
export class FilterOperator<T> extends LinearUnaryOperator<T, T> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    f: (data: T) => boolean,
    initialFrontier: Antichain,
  ) {
    const filterInner = (collection: MultiSet<T>) => collection.filter(f)
    super(id, inputA, output, filterInner, initialFrontier)
  }
}

/**
 * Operator that negates the multiplicities in the input stream
 */
export class NegateOperator<T> extends LinearUnaryOperator<T, T> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    const negateInner = (collection: MultiSet<T>) => collection.negate()
    super(id, inputA, output, negateInner, initialFrontier)
  }
}

/**
 * Operator that concatenates two input streams
 */
export class ConcatOperator<T, T2> extends BinaryOperator<T | T2> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    inputB: DifferenceStreamReader<T2>,
    output: DifferenceStreamWriter<T | T2>,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
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

    super(id, inputA, inputB, output, inner, initialFrontier)
  }
}

/**
 * Operator that logs debug information about the stream
 */
export class DebugOperator<T> extends UnaryOperator<T> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    name: string,
    initialFrontier: Antichain,
    indent: boolean = false,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<T>
          console.log(
            `debug ${name} data: version: ${version.toString()} collection: ${collection.toString(indent)}`,
          )
          this.output.sendData(version, collection)
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.inputFrontier().lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputFrontier(frontier)
          console.log(
            `debug ${name} notification: frontier ${frontier.toString()}`,
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

    super(id, inputA, output, inner, initialFrontier)
  }
}

/**
 * Operator that outputs the messages in the stream
 */
export class OutputOperator<T> extends UnaryOperator<T> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    fn: (data: Message<T>) => void,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        fn(message)
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
    super(id, inputA, output, inner, initialFrontier)
  }
}

/**
 * Operator that consolidates collections at each version
 */
export class ConsolidateOperator<T> extends UnaryOperator<T> {
  #collections = new DefaultMap<Version, MultiSet<T>>(() => new MultiSet<T>())

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
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
        const consolidated = collection.consolidate()
        this.#collections.delete(version)
        this.output.sendData(version, consolidated)
      }

      if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
        throw new Error('Invalid frontier state')
      }
      if (this.outputFrontier.lessThan(this.inputFrontier())) {
        this.outputFrontier = this.inputFrontier()
        this.output.sendFrontier(this.outputFrontier)
      }
    }

    super(id, inputA, output, inner, initialFrontier)
  }
}

/**
 * Operator that joins two input streams
 */
export class JoinOperator<K, V1, V2> extends BinaryOperator<[K, unknown]> {
  #indexA = new Index<K, V1>()
  #indexB = new Index<K, V2>()

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    inputB: DifferenceStreamReader<[K, V2]>,
    output: DifferenceStreamWriter<[K, [V1, V2]]>,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
      const deltaA = new Index<K, V1>()
      const deltaB = new Index<K, V2>()

      // Process input A
      for (const message of this.inputAMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<[K, V1]>
          for (const [item, multiplicity] of collection.getInner()) {
            const [key, value] = item
            deltaA.addValue(key, version, [value, multiplicity])
          }
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.inputAFrontier().lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputAFrontier(frontier)
        }
      }

      // Process input B
      for (const message of this.inputBMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<[K, V2]>
          for (const [item, multiplicity] of collection.getInner()) {
            const [key, value] = item
            deltaB.addValue(key, version, [value, multiplicity])
          }
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.inputBFrontier().lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputBFrontier(frontier)
        }
      }

      // Process results
      const results = new Map<Version, MultiSet<[K, [V1, V2]]>>()

      // Join deltaA with existing indexB
      for (const [version, collection] of deltaA.join(this.#indexB)) {
        const existing = results.get(version) || new MultiSet<[K, [V1, V2]]>()
        existing.extend(collection)
        results.set(version, existing)
      }

      // Append deltaA to indexA
      this.#indexA.append(deltaA)

      // Join existing indexA with deltaB
      for (const [version, collection] of this.#indexA.join(deltaB)) {
        const existing = results.get(version) || new MultiSet<[K, [V1, V2]]>()
        existing.extend(collection)
        results.set(version, existing)
      }

      // Send results
      for (const [version, collection] of results) {
        this.output.sendData(version, collection)
      }

      // Append deltaB to indexB
      this.#indexB.append(deltaB)

      // Update frontiers
      const inputFrontier = this.inputAFrontier().meet(this.inputBFrontier())
      if (!this.outputFrontier.lessEqual(inputFrontier)) {
        throw new Error('Invalid frontier state')
      }
      if (this.outputFrontier.lessThan(inputFrontier)) {
        this.outputFrontier = inputFrontier
        this.output.sendFrontier(this.outputFrontier)
        this.#indexA.compact(this.outputFrontier)
        this.#indexB.compact(this.outputFrontier)
      }
    }

    super(id, inputA, inputB, output, inner, initialFrontier)
  }
}

/**
 * Base operator for reduction operations
 */
export class ReduceOperator<K, V1, V2> extends UnaryOperator<[K, V1 | V2]> {
  #index = new Index<K, V1>()
  #indexOut = new Index<K, V2>()
  #keysTodo = new Map<Version, Set<K>>()

  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V1]>,
    output: DifferenceStreamWriter<[K, V2]>,
    f: (values: [V1, number][]) => [V2, number][],
    initialFrontier: Antichain,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<[K, V1]>
          for (const [item, multiplicity] of collection.getInner()) {
            const [key, value] = item
            this.#index.addValue(key, version, [value, multiplicity])

            let todoSet = this.#keysTodo.get(version)
            if (!todoSet) {
              todoSet = new Set<K>()
              this.#keysTodo.set(version, todoSet)
            }
            todoSet.add(key)

            // Add key to all join versions
            for (const v2 of this.#index.versions(key)) {
              const joinVersion = version.join(v2)
              let joinTodoSet = this.#keysTodo.get(joinVersion)
              if (!joinTodoSet) {
                joinTodoSet = new Set<K>()
                this.#keysTodo.set(joinVersion, joinTodoSet)
              }
              joinTodoSet.add(key)
            }
          }
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.inputFrontier().lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputFrontier(frontier)
        }
      }

      // Find versions that are complete
      const finishedVersions = Array.from(this.#keysTodo.entries())
        .filter(([version]) => !this.inputFrontier().lessEqualVersion(version))
        .sort(([a], [b]) => {
          return a.lessEqual(b) ? -1 : 1
        })

      for (const [version, keys] of finishedVersions) {
        const result: [[K, V2], number][] = []

        for (const key of keys) {
          const curr = this.#index.reconstructAt(key, version)
          const currOut = this.#indexOut.reconstructAt(key, version)
          const out = f(curr)

          // Calculate delta between current and previous output
          const delta = new Map<string, number>()
          const values = new Map<string, V2>()
          for (const [value, multiplicity] of out) {
            const valueKey = JSON.stringify(value)
            values.set(valueKey, value)
            delta.set(valueKey, (delta.get(valueKey) || 0) + multiplicity)
          }
          for (const [value, multiplicity] of currOut) {
            const valueKey = JSON.stringify(value)
            values.set(valueKey, value)
            delta.set(valueKey, (delta.get(valueKey) || 0) - multiplicity)
          }

          // Add non-zero deltas to result
          for (const [valueKey, multiplicity] of delta) {
            const value = values.get(valueKey)!
            if (multiplicity !== 0) {
              result.push([[key, value], multiplicity])
              this.#indexOut.addValue(key, version, [value, multiplicity])
            }
          }
        }

        if (result.length > 0) {
          this.output.sendData(version, new MultiSet(result))
        }
        this.#keysTodo.delete(version)
      }

      if (!this.outputFrontier.lessEqual(this.inputFrontier())) {
        throw new Error('Invalid frontier state')
      }
      if (this.outputFrontier.lessThan(this.inputFrontier())) {
        this.outputFrontier = this.inputFrontier()
        this.output.sendFrontier(this.outputFrontier)
        this.#index.compact(this.outputFrontier)
        this.#indexOut.compact(this.outputFrontier)
      }
    }

    super(id, inputA, output, inner, initialFrontier)
  }
}

/**
 * Operator that counts elements by key
 */
export class CountOperator<K, V> extends ReduceOperator<K, V, number> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, number]>,
    initialFrontier: Antichain,
  ) {
    const countInner = (vals: [V, number][]): [number, number][] => {
      let count = 0
      for (const [_, diff] of vals) {
        count += diff
      }
      return [[count, 1]]
    }

    super(id, inputA, output, countInner, initialFrontier)
  }
}

/**
 * Operator that removes duplicates by key
 */
export class DistinctOperator<K, V> extends ReduceOperator<K, V, V> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<[K, V]>,
    output: DifferenceStreamWriter<[K, V]>,
    initialFrontier: Antichain,
  ) {
    const distinctInner = (vals: [V, number][]): [V, number][] => {
      const consolidated = new Map<string, number>()
      const values = new Map<string, V>()
      for (const [val, diff] of vals) {
        const key = JSON.stringify(val)
        consolidated.set(key, (consolidated.get(key) || 0) + diff)
        values.set(key, val)
      }
      return Array.from(consolidated.entries())
        .filter(([_, count]) => count > 0)
        .map(([key, _]) => [values.get(key) as V, 1])
    }

    super(id, inputA, output, distinctInner, initialFrontier)
  }
}

/**
 * Operator that moves data into a new iteration scope
 */
export class IngressOperator<T> extends UnaryOperator<T> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<T>
          const newVersion = version.extend()
          this.output.sendData(newVersion, collection)
          this.output.sendData(newVersion.applyStep(1), collection.negate())
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          const newFrontier = frontier.extend()
          if (!this.inputFrontier().lessEqual(newFrontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputFrontier(newFrontier)
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

    super(id, inputA, output, inner, initialFrontier)
  }
}

/**
 * Operator that moves data out of an iteration scope
 */
export class EgressOperator<T> extends UnaryOperator<T> {
  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<T>
          const newVersion = version.truncate()
          this.output.sendData(newVersion, collection)
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          const newFrontier = frontier.truncate()
          if (!this.inputFrontier().lessEqual(newFrontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputFrontier(newFrontier)
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

    super(id, inputA, output, inner, initialFrontier)
  }
}

/**
 * Operator that handles feedback in iteration loops.
 * This operator is responsible for:
 * 1. Incrementing versions for feedback data
 * 2. Managing the iteration state
 * 3. Determining when iterations are complete
 */
export class FeedbackOperator<T> extends UnaryOperator<T> {
  // Map from top-level version -> set of messages where we have
  // sent some data at that version
  #inFlightData = new DefaultMap<Version, Set<Version>>(() => new Set())
  // Versions where a given top-level version has updated
  // its iteration without sending any data.
  #emptyVersions = new DefaultMap<Version, Set<Version>>(() => new Set())

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    step: number,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    const inner = () => {
      for (const message of this.inputMessages()) {
        if (message.type === MessageType.DATA) {
          const { version, collection } = message.data as DataMessage<T>
          const newVersion = version.applyStep(step)
          const truncated = newVersion.truncate()
          this.output.sendData(newVersion, collection)

          // Record that we sent data at this version
          this.#inFlightData.get(truncated).add(newVersion)
        } else if (message.type === MessageType.FRONTIER) {
          const frontier = message.data as Antichain
          if (!this.inputFrontier().lessEqual(frontier)) {
            throw new Error('Invalid frontier update')
          }
          this.setInputFrontier(frontier)
        }
      }

      // Increment the current input frontier
      const incrementedInputFrontier = this.inputFrontier().applyStep(step)
      // Grab all of the elements from the potential output frontier
      const elements = incrementedInputFrontier.elements
      // Partition every element from this potential output frontier into one of
      // two sets, either elements to keep, or elements to reject
      const candidateOutputFrontier: Version[] = []
      const rejected: Version[] = []

      for (const elem of elements) {
        const truncated = elem.truncate()
        const inFlightSet = this.#inFlightData.get(truncated)

        // Always keep a frontier element if there is are differences associated
        // with its top-level version that are still in flight
        if (inFlightSet.size > 0) {
          candidateOutputFrontier.push(elem)

          // Clean up versions that will be closed by this frontier element
          for (const version of inFlightSet) {
            if (version.lessThan(elem)) {
              inFlightSet.delete(version)
            }
          }
        } else {
          // This frontier element does not have any differences associated with its
          // top-level version that were not closed out by prior frontier updates

          // Remember that we observed an "empty" update for this top-level version
          const emptySet = this.#emptyVersions.get(truncated)
          emptySet.add(elem)

          // Don't do anything if we haven't observed at least three "empty" frontier
          // updates for this top-level time
          if (emptySet.size <= 3) {
            candidateOutputFrontier.push(elem)
          } else {
            this.#inFlightData.delete(truncated)
            this.#emptyVersions.delete(truncated)
            rejected.push(elem)
          }
        }
      }

      // Ensure that we can still send data at all other top-level
      // versions that were not rejected
      for (const r of rejected) {
        for (const inFlightSet of this.#inFlightData.values()) {
          for (const version of inFlightSet) {
            candidateOutputFrontier.push(r.join(version))
          }
        }
      }

      // Construct a minimal antichain from the set of candidate elements
      const outputFrontier = new Antichain(candidateOutputFrontier)

      if (!this.outputFrontier.lessEqual(outputFrontier)) {
        throw new Error('Invalid frontier state')
      }
      if (this.outputFrontier.lessThan(outputFrontier)) {
        this.outputFrontier = outputFrontier
        this.output.sendFrontier(this.outputFrontier)
      }
    }

    super(id, inputA, output, inner, initialFrontier)
  }
}
