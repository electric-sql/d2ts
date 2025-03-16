import { describe, expect, test } from 'vitest'
import {
  Message,
  MessageType,
  DataMessage,
  FrontierMessage,
} from '../src/types.js'
import { MultiSet } from '../src/multiset.js'
import { D2 } from '../src/d2.js'
import { Cache } from '../src/cache.js'
import { map } from '../src/operators/map.js'
import { output } from '../src/operators/output.js'
import { Antichain } from '../src/order.js'

describe('Cache', () => {
  test('sends initial data to subscribers', () => {
    const baseGraph = new D2({ initialFrontier: 0 })
    const baseInput = baseGraph.newInput<[string, number]>()
    const mapped = baseInput.pipe(
      map(([key, value]) => [key, value * 2] as [string, number]),
    )
    const cache = new Cache(mapped)
    baseGraph.finalize()

    baseInput.sendData(
      0,
      new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
      ]),
    )
    baseInput.sendFrontier(1)

    baseGraph.run()

    const newGraph = new D2({ initialFrontier: 0 })
    const messages: Message<[string, number]>[] = []

    cache.pipeInto(newGraph).pipe(output((message) => messages.push(message)))
    newGraph.finalize()
    newGraph.run()

    expect(messages.length).toEqual(2)

    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([1])
      expect(data.collection.getInner()).toEqual([
        [['a', 2], 1],
        [['b', 4], 1],
      ])
    }
    {
      const msg2 = messages[1]
      expect(msg2.type).toEqual(MessageType.FRONTIER)
    }
  })

  test('sends incrimental data to subscribers', () => {
    const baseGraph = new D2({ initialFrontier: 0 })
    const baseInput = baseGraph.newInput<[string, number]>()
    const mapped = baseInput.pipe(
      map(([key, value]) => [key, value * 2] as [string, number]),
    )
    const cache = new Cache(mapped)
    baseGraph.finalize()

    baseGraph.run()

    const newGraph = new D2({ initialFrontier: 0 })
    const messages: Message<[string, number]>[] = []

    cache.pipeInto(newGraph).pipe(output((message) => messages.push(message)))
    newGraph.finalize()
    newGraph.run()

    expect(messages.length).toEqual(0)

    baseInput.sendData(
      1,
      new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
      ]),
    )
    baseInput.sendFrontier(2)

    baseGraph.run()
    newGraph.run()

    expect(messages.length).toEqual(2)

    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([1])
      expect(data.collection.getInner()).toEqual([
        [['a', 2], 1],
        [['b', 4], 1],
      ])
    }
    {
      const msg2 = messages[1]
      expect(msg2.type).toEqual(MessageType.FRONTIER)
    }
  })
})
