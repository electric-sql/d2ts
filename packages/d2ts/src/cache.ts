import {
  DataMessage,
  ID2,
  IStreamBuilder,
  Message,
  MessageType,
  KeyValue,
} from './types.js'
import { Index } from './version-index.js'
import { output } from './operators/output.js'
import { Antichain, Version } from './order.js'
import { MultiSet } from './multiset.js'
import { DefaultMap } from './utils.js'
import { filter } from './operators/filter.js'
import { eq, IndexOperator } from './index-operators.js'

export interface PipeIntoOptions<K, _V> {
  whereKey?: K | IndexOperator<K>
}

export class Cache<K, V> {
  #index = new Index<K, V>()
  #stream: IStreamBuilder<KeyValue<K, V>>
  #subscribers = new Set<IStreamBuilder<KeyValue<K, V>>>()

  constructor(
    stream: IStreamBuilder<KeyValue<K, V>>,
  ) {
    this.#stream = stream
    this.#stream.pipe(
      output((message) => {
        this.#handleInputMessage(message)
      }),
    )
  }

  #handleInputMessage(message: Message<KeyValue<K, V>>): void {
    if (message.type === MessageType.DATA) {
      const { version, collection } = message.data as DataMessage<
        KeyValue<K, V>
      >
      for (const [item, multiplicity] of collection.getInner()) {
        const [key, value] = item
        this.#index.addValue(key, version, [value, multiplicity])
      }
    } else if (message.type === MessageType.FRONTIER) {
      const frontier = message.data as Antichain
      this.#index.compact(frontier)
    }
    this.#broadcast(message)
  }

  #broadcast(message: Message<KeyValue<K, V>>): void {
    if (message.type === MessageType.DATA) {
      const { version, collection } = message.data
      for (const subscriber of this.#subscribers) {
        subscriber.writer.sendData(version, collection)
      }
    } else if (message.type === MessageType.FRONTIER) {
      const frontier = message.data as Antichain
      this.#index.compact(frontier)
      for (const subscriber of this.#subscribers) {
        subscriber.writer.sendFrontier(frontier)
      }
    }
  }

  #sendHistory(
    input: IStreamBuilder<KeyValue<K, V>>,
    options: PipeIntoOptions<K, V> = {},
  ): void {
    const versionedData = new DefaultMap<Version, [[K, V], number][]>(() => [])

    let keysToSend: K[]
    if (options.whereKey) {
      if (typeof options.whereKey === 'function') {
        keysToSend = this.#index.matchKeys(options.whereKey as IndexOperator<K>)
      } else {
        keysToSend = [options.whereKey]
      }
    } else {
      keysToSend = this.#index.keys()
    }

    for (const key of keysToSend) {
      for (const [version, values] of this.#index.get(key)) {
        for (const [value, multiplicity] of values) {
          versionedData.get(version).push([[key, value], multiplicity])
        }
      }
    }
    const sortedVersions = Array.from(versionedData.keys()).sort((a, b) =>
      a.lessThan(b) ? -1 : 1,
    )
    for (const version of sortedVersions) {
      input.writer.sendData(version, new MultiSet(versionedData.get(version)))
    }
    const frontier = this.#index.frontier
    if (frontier) {
      input.writer.sendFrontier(frontier)
    }
  }

  pipeInto(
    graph: ID2,
    options: PipeIntoOptions<K, V> = {},
  ): IStreamBuilder<KeyValue<K, V>> {
    const input = graph.newInput<KeyValue<K, V>>()
    this.#subscribers.add(input)

    graph.addStartupSubscriber(() => {
      this.#sendHistory(input, options)
    })

    let pipeline = input

    if (options.whereKey) {
      const operator =
        typeof options.whereKey === 'function'
          ? (options.whereKey as IndexOperator<K>)
          : (eq(options.whereKey) as IndexOperator<K>)
      pipeline = pipeline.pipe(filter(([key]) => operator(key)))
    }

    graph.addTeardownSubscriber(() => {
      this.#subscribers.delete(input)
    })

    return pipeline
  }
}
