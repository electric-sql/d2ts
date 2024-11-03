import { MultiSet } from './multiset'
import { Version, Antichain } from './order'
import { Index } from './version-index'
import {
  DifferenceStreamReader,
  DifferenceStreamWriter,
  UnaryOperator,
  BinaryOperator,
  MessageType,
} from './graph'

export type KeyedData<T> = {
  key: string
  value: T
}

interface Versioned<T> {
  version: Version
  collection: MultiSet<T>
}

/**
 * An operator that applies a function to each element in the input stream.
 * The output stream contains the transformed elements.
 */
export class MapOperator<T, U> extends UnaryOperator<T> {
  #f: (data: T) => U

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<U>,
    f: (data: T) => U,
    initialFrontier: Version,
  ) {
    super(
      input,
      output as unknown as DifferenceStreamWriter<T>,
      () => this.#process(),
      initialFrontier,
    )
    this.#f = f
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        ;(this.output as unknown as DifferenceStreamWriter<U>).sendData(
          version,
          collection.map(this.#f),
        )
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        this.setInputFrontier(frontier)
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(new Antichain([this.outputFrontier]))
        }
      }
    }
  }
}

/**
 * An operator that filters elements from the input stream based on a predicate.
 * Only elements for which the predicate returns true are included in the output stream.
 */
export class FilterOperator<T> extends UnaryOperator<T> {
  #f: (data: T) => boolean

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    f: (data: T) => boolean,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
    this.#f = f
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        this.output.sendData(version, collection.filter(this.#f))
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        this.setInputFrontier(frontier)
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(new Antichain([this.outputFrontier]))
        }
      }
    }
  }
}

/**
 * An operator that negates the multiplicities of all elements in the input stream.
 * Each element in the output stream has its multiplicity negated.
 */
export class NegateOperator<T> extends UnaryOperator<T> {
  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        this.output.sendData(version, collection.negate())
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        this.setInputFrontier(frontier)
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(new Antichain([this.outputFrontier]))
        }
      }
    }
  }
}

/**
 * An operator that concatenates two input streams into a single output stream.
 * Elements from both input streams are included in the output stream with their original multiplicities.
 */
export class ConcatOperator<T> extends BinaryOperator<T> {
  constructor(
    inputA: DifferenceStreamReader<T>,
    inputB: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Version,
  ) {
    super(inputA, inputB, output, () => this.#process(), initialFrontier)
  }

  #process(): void {
    for (const { type, data } of this.inputAMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        this.output.sendData(version, collection)
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        this.setInputAFrontier(frontier)
      }
    }

    for (const { type, data } of this.inputBMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        this.output.sendData(version, collection)
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        this.setInputBFrontier(frontier)
      }
    }

    const inputFrontier = this.inputAFrontier().meet(this.inputBFrontier())
    if (this.outputFrontier.lessThan(inputFrontier)) {
      this.outputFrontier = inputFrontier
      this.output.sendFrontier(new Antichain([this.outputFrontier]))
    }
  }
}

/**
 * An operator that outputs debug information about the elements flowing through it.
 * Elements are passed through unchanged, but their values and versions are logged.
 */
export class DebugOperator<T> extends UnaryOperator<T> {
  #name: string

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    name: string,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
    this.#name = name
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        console.log(
          `debug ${this.#name} data: version: ${version} collection: ${collection}`,
        )
        this.output.sendData(version, collection)
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        console.log(`debug ${this.#name} notification: frontier ${frontier}`)
        this.setInputFrontier(frontier)
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(new Antichain([this.outputFrontier]))
        }
      }
    }
  }
}

/**
 * An operator that joins two streams based on their keys.
 * For each pair of elements with matching keys, outputs a new element containing both values.
 * The multiplicity of the output element is the product of the input multiplicities.
 *
 * @typeParam K - The type of the key used for joining
 * @typeParam V1 - The type of values in the first input stream
 * @typeParam V2 - The type of values in the second input stream
 */
export class JoinOperator<K, V1, V2> extends BinaryOperator<
  KeyedData<V1> | KeyedData<V2>
> {
  #indexA = new Index<K, V1>()
  #indexB = new Index<K, V2>()

  constructor(
    inputA: DifferenceStreamReader<KeyedData<V1>>,
    inputB: DifferenceStreamReader<KeyedData<V2>>,
    output: DifferenceStreamWriter<KeyedData<[V1, V2]>>,
    initialFrontier: Version,
  ) {
    super(
      inputA as DifferenceStreamReader<KeyedData<V1> | KeyedData<V2>>,
      inputB as DifferenceStreamReader<KeyedData<V1> | KeyedData<V2>>,
      output as unknown as DifferenceStreamWriter<
        KeyedData<V1> | KeyedData<V2>
      >,
      () => this.#process(),
      initialFrontier,
    )
  }

  #process(): void {
    const deltaA = new Index<string, V1>()
    const deltaB = new Index<string, V2>()

    for (const { type, data } of this.inputAMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as unknown as Versioned<
          KeyedData<V1>
        >
        for (const [{ key, value }, multiplicity] of collection.getInner()) {
          deltaA.addValue(key as string, version, [value, multiplicity])
        }
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        this.setInputAFrontier(frontier)
      }
    }

    for (const { type, data } of this.inputBMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as unknown as Versioned<
          KeyedData<V2>
        >
        for (const [{ key, value }, multiplicity] of collection.getInner()) {
          deltaB.addValue(key as string, version, [value, multiplicity])
        }
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as unknown as Version
        this.setInputBFrontier(frontier)
      }
    }

    const results = new Map<Version, MultiSet<KeyedData<[V1, V2]>>>()

    for (const [version, collection] of deltaA.join(this.#indexB)) {
      if (!results.has(version)) {
        results.set(version, new MultiSet())
      }
      const transformed = collection.map(([k, [v1, v2]]) => ({
        key: k,
        value: [v1, v2] as [V1, V2],
      }))
      results.get(version)!.extend(transformed.getInner())
    }

    this.#indexA.append(deltaA)

    for (const [version, collection] of this.#indexA.join(deltaB)) {
      if (!results.has(version)) {
        results.set(version, new MultiSet())
      }
      results.get(version)!.extend(collection.getInner())
    }

    for (const [version, collection] of results) {
      ;(
        this.output as unknown as DifferenceStreamWriter<KeyedData<[V1, V2]>>
      ).sendData(version, collection)
    }

    this.#indexB.append(deltaB)

    const inputFrontier = new Antichain([
      this.inputAFrontier(),
      this.inputBFrontier(),
    ])

    if (this.outputFrontier.lessThan(inputFrontier)) {
      this.outputFrontier = inputFrontier
      this.output.sendFrontier(new Antichain([this.outputFrontier]))
      this.#indexA.compact(new Antichain([this.outputFrontier]))
      this.#indexB.compact(new Antichain([this.outputFrontier]))
    }
  }
}

/**
 * An operator that counts the number of occurrences of each key in the input stream.
 * The output stream contains elements with the count as their value.
 */
export class CountOperator<T> extends UnaryOperator<T> {
  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<number>,
    initialFrontier: Version,
  ) {
    super(
      input,
      output as unknown as DifferenceStreamWriter<T>,
      () => this.#process(),
      initialFrontier,
    )
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as Versioned<T>
        ;(this.output as unknown as DifferenceStreamWriter<number>).sendData(
          version,
          collection.count(),
        )
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as Version
        this.setInputFrontier(frontier)
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(new Antichain([this.outputFrontier]))
        }
      }
    }
  }
}

/**
 * An operator that consolidates elements with the same key by combining their multiplicities.
 * Elements with zero total multiplicity are removed from the output stream.
 */
export class ConsolidateOperator<T> extends UnaryOperator<T> {
  #collections = new Map<Version, MultiSet<T>>()

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        if (!this.#collections.has(version)) {
          this.#collections.set(version, new MultiSet())
        }
        this.#collections.get(version)!.extend(collection.getInner())
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as Version
        this.setInputFrontier(frontier)
      }
    }

    const finishedVersions = Array.from(this.#collections.keys()).filter(
      (version) => !this.inputFrontier().lessEqual(version),
    )

    for (const version of finishedVersions) {
      const collection = this.#collections.get(version)!.consolidate()
      this.#collections.delete(version)
      this.output.sendData(version, collection)
    }

    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(new Antichain([this.outputFrontier]))
    }
  }
}

/**
 * An operator that produces a set of distinct elements from the input stream.
 * Each key appears at most once in the output stream with multiplicity 1.
 * Negative multiplicities in the input stream will cause an error.
 */
export class DistinctOperator<T> extends UnaryOperator<T> {
  #collections = new Map<Version, MultiSet<T>>()

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        if (!this.#collections.has(version)) {
          this.#collections.set(version, new MultiSet())
        }
        this.#collections.get(version)!.extend(collection.getInner())
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as Version
        this.setInputFrontier(frontier)
      }
    }

    const finishedVersions = Array.from(this.#collections.keys()).filter(
      (version) => !this.inputFrontier().lessEqual(version),
    )

    for (const version of finishedVersions) {
      const collection = this.#collections.get(version)!.distinct()
      this.#collections.delete(version)
      this.output.sendData(version, collection)
    }

    if (this.outputFrontier.lessThan(this.inputFrontier())) {
      this.outputFrontier = this.inputFrontier()
      this.output.sendFrontier(new Antichain([this.outputFrontier]))
    }
  }
}

/**
 * An operator that extends the version of elements entering a scope.
 * Used as part of the iterate implementation to manage nested scopes.
 */
export class IngressOperator<T> extends UnaryOperator<T> {
  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        const newVersion = version.extend()
        this.output.sendData(newVersion, collection)
        this.output.sendData(newVersion.applyStep(1), collection.negate())
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as Version
        const newFrontier = frontier.extend()
        this.setInputFrontier(newFrontier)
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(new Antichain([this.outputFrontier]))
        }
      }
    }
  }
}

/**
 * An operator that truncates the version of elements leaving a scope.
 * Used as part of the iterate implementation to manage nested scopes.
 */
export class EgressOperator<T> extends UnaryOperator<T> {
  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
  }

  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        const newVersion = version.truncate()
        this.output.sendData(newVersion, collection)
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as Version
        const newFrontier = frontier.truncate()
        this.setInputFrontier(newFrontier)
        if (this.outputFrontier.lessThan(this.inputFrontier())) {
          this.outputFrontier = this.inputFrontier()
          this.output.sendFrontier(new Antichain([this.outputFrontier]))
        }
      }
    }
  }
}

/**
 * An operator that implements feedback in iterative computations.
 * It manages the coordination between iterations by:
 * 1. Advancing versions by the step size
 * 2. Tracking in-flight data
 * 3. Managing empty version notifications
 * 4. Determining when iteration has reached a fixed point
 *
 * @typeParam T - The type of elements in the stream
 */
export class FeedbackOperator<T> extends UnaryOperator<T> {
  #step: number
  #inFlightData = new Map<Version, Set<Version>>()
  #emptyVersions = new Map<Version, Set<Version>>()

  constructor(
    input: DifferenceStreamReader<T>,
    step: number,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Version,
  ) {
    super(input, output, () => this.#process(), initialFrontier)
    this.#step = step
  }

  /**
   * Processes messages from the input stream:
   * - For data messages: advances the version and tracks the data
   * - For frontier messages: updates frontiers and manages iteration state
   * - Determines when to terminate iteration based on empty notifications
   */
  #process(): void {
    for (const { type, data } of this.inputMessages()) {
      if (type === MessageType.DATA) {
        const { version, collection } = data as {
          version: Version
          collection: MultiSet<T>
        }
        const newVersion = version.applyStep(this.#step)
        const truncated = newVersion.truncate()
        this.output.sendData(newVersion, collection)

        if (!this.#inFlightData.has(truncated)) {
          this.#inFlightData.set(truncated, new Set())
        }
        this.#inFlightData.get(truncated)!.add(newVersion)

        if (!this.#emptyVersions.has(truncated)) {
          this.#emptyVersions.set(truncated, new Set())
        }
      } else if (type === MessageType.FRONTIER) {
        const frontier = data as Version
        this.setInputFrontier(frontier)
      }
    }

    const incrementedInputFrontier = this.inputFrontier().applyStep(this.#step)
    const elements = [incrementedInputFrontier]
    const candidateOutputFrontier: Version[] = []
    const rejected: Version[] = []

    for (const elem of elements) {
      const truncated = elem.truncate()

      if (
        this.#inFlightData.has(truncated) &&
        this.#inFlightData.get(truncated)!.size > 0
      ) {
        candidateOutputFrontier.push(elem)

        const closed = new Set(
          Array.from(this.#inFlightData.get(truncated)!).filter((x) =>
            x.lessThan(elem),
          ),
        )
        closed.forEach((x) => this.#inFlightData.get(truncated)!.delete(x))
      } else {
        if (!this.#emptyVersions.has(truncated)) {
          this.#emptyVersions.set(truncated, new Set())
        }
        this.#emptyVersions.get(truncated)!.add(elem)

        if (this.#emptyVersions.get(truncated)!.size <= 3) {
          candidateOutputFrontier.push(elem)
        } else {
          this.#inFlightData.delete(truncated)
          this.#emptyVersions.delete(truncated)
          rejected.push(elem)
        }
      }
    }

    for (const r of rejected) {
      for (const truncated of this.#inFlightData.keys()) {
        candidateOutputFrontier.push(r.join(truncated.extend()))
      }
    }

    const candidateOutputFrontierAntichain = new Antichain(
      candidateOutputFrontier,
    )

    if (this.outputFrontier.lessThan(candidateOutputFrontierAntichain)) {
      this.outputFrontier = candidateOutputFrontierAntichain
      this.output.sendFrontier(new Antichain([this.outputFrontier]))
    }
  }
}
