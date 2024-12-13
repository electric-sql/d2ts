import { IStreamBuilder, DataMessage, MessageType } from '../types.js'
import { DifferenceStreamReader, DifferenceStreamWriter, UnaryOperator } from '../graph.js'
import { StreamBuilder } from '../d2.js'
import { Antichain, Version } from '../order.js'
import { DefaultMap } from '../utils.js'
import { concat } from './concat.js'

/**
 * Operator that moves data into a new iteration scope
 */
export class IngressOperator<T> extends UnaryOperator<T> {
  run(): void {
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
}

/**
 * Operator that moves data out of an iteration scope
 */
export class EgressOperator<T> extends UnaryOperator<T> {
  run(): void {
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
  #step: number

  constructor(
    id: number,
    inputA: DifferenceStreamReader<T>,
    step: number,
    output: DifferenceStreamWriter<T>,
    initialFrontier: Antichain,
  ) {
    super(id, inputA, output, initialFrontier)
    this.#step = step
  }

  run(): void {
    for (const message of this.inputMessages()) {
      if (message.type === MessageType.DATA) {
        const { version, collection } = message.data as DataMessage<T>
        const newVersion = version.applyStep(this.#step)
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
    const incrementedInputFrontier = this.inputFrontier().applyStep(this.#step)
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
}

function ingress<T>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter(),
    )
    const operator = new IngressOperator<T>(
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

function egress<T>() {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    const output = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter(),
    )
    const operator = new EgressOperator<T>(
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

/**
 * Iterates over the stream
 */
export function iterate<T>(
  f: (stream: IStreamBuilder<T>) => IStreamBuilder<T>,
) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    // Enter scope
    const newFrontier = stream.graph.frontier().extend()
    stream.graph.pushFrontier(newFrontier)

    const feedbackStream = new StreamBuilder<T>(
      stream.graph,
      new DifferenceStreamWriter(),
    )
    const entered = stream.pipe(ingress(), concat(feedbackStream))
    const result = f(entered)
    const feedbackOperator = new FeedbackOperator<T>(
      stream.graph.getNextOperatorId(),
      result.connectReader() as DifferenceStreamReader<T>,
      1,
      feedbackStream.writer,
      stream.graph.frontier(),
    )
    stream.graph.addStream(feedbackStream.connectReader())
    stream.graph.addOperator(feedbackOperator)

    // Exit scope
    stream.graph.popFrontier()

    return result.pipe(egress())
  }
} 