import { describe, expect, test } from 'vitest'
import {
  Message,
  MessageType,
  DataMessage,
} from '../src/types.js'
import { MultiSet } from '../src/multiset.js'
import { D2 } from '../src/d2.js'
import { Cache } from '../src/cache.js'
import { map } from '../src/operators/map.js'
import { output } from '../src/operators/output.js'
import { gt, lt, eq, or, and, between, isIn } from '../src/index-operators.js'

describe('Cache', () => {
  test('sends all initial data to subscribers', () => {
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

  test('sends all incremental data to subscribers', () => {
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

  test('sends initial data that matched whereKey to subscribers', () => {
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

    cache
      .pipeInto(newGraph, { whereKey: 'a' })
      .pipe(output((message) => messages.push(message)))
    newGraph.finalize()
    newGraph.run()

    expect(messages.length).toEqual(2)

    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([1])
      expect(data.collection.getInner()).toEqual([[['a', 2], 1]])
    }
    {
      const msg2 = messages[1]
      expect(msg2.type).toEqual(MessageType.FRONTIER)
    }
  })

  test('sends all incremental that matched whereKey data to subscribers', () => {
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

    cache
      .pipeInto(newGraph, { whereKey: 'a' })
      .pipe(output((message) => messages.push(message)))
    newGraph.finalize()
    newGraph.run()

    expect(messages.length).toEqual(0)

    baseInput.sendData(1, new MultiSet<[string, number]>([[['a', 1], 1]]))
    baseInput.sendFrontier(2)

    baseGraph.run()
    newGraph.run()

    expect(messages.length).toEqual(2)

    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([1])
      expect(data.collection.getInner()).toEqual([[['a', 2], 1]])
    }
    {
      const msg2 = messages[1]
      expect(msg2.type).toEqual(MessageType.FRONTIER)
    }
  })

  test('supports gt operator for whereKey', () => {
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
        [['c', 3], 1],
      ]),
    )
    baseInput.sendFrontier(1)

    baseGraph.run()

    const newGraph = new D2({ initialFrontier: 0 })
    const messages: Message<[string, number]>[] = []

    cache
      .pipeInto(newGraph, { whereKey: gt('b') })
      .pipe(output((message) => messages.push(message)))
    newGraph.finalize()
    newGraph.run()

    expect(messages.length).toEqual(2)

    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([1])
      expect(data.collection.getInner()).toEqual([[['c', 6], 1]])
    }

    // Test incremental updates
    messages.length = 0

    // Send data that matches the operator (> 'a')
    baseInput.sendData(2, new MultiSet<[string, number]>([[['d', 4], 1]]))
    // Send data that doesn't match the operator (<= 'a')
    baseInput.sendData(2, new MultiSet<[string, number]>([[['b', 5], 1]]))
    baseInput.sendFrontier(3)

    baseGraph.run()
    newGraph.run()

    expect(messages.length).toEqual(3)
    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([2])
      // Should only include 'd' as it's > 'a', not 'a' itself
      expect(data.collection.getInner()).toEqual([[['d', 8], 1]])
    }
  })

  test('supports between operator for whereKey', () => {
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
        [['d', 4], 1],
      ]),
    )
    baseInput.sendFrontier(1)

    baseGraph.run()

    const newGraph = new D2({ initialFrontier: 0 })
    const messages: Message<[string, number]>[] = []

    cache
      .pipeInto(newGraph, { whereKey: between('b', 'd') })
      .pipe(output((message) => messages.push(message)))
    newGraph.finalize()
    newGraph.run()

    expect(messages.length).toEqual(2)

    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([1])
      expect(data.collection.getInner()).toEqual([
        [['b', 4], 1],
        [['d', 8], 1],
      ])
    }

    // Test incremental updates
    messages.length = 0

    // Send data that matches the between operator
    baseInput.sendData(2, new MultiSet<[string, number]>([[['c', 5], 1]]))
    // Send data that doesn't match the between operator
    baseInput.sendData(2, new MultiSet<[string, number]>([[['e', 7], 1]]))
    baseInput.sendFrontier(3)

    baseGraph.run()
    newGraph.run()

    expect(messages.length).toEqual(3)
    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([2])
      // Should only include 'b' as it's between 'b' and 'c'
      expect(data.collection.getInner()).toEqual([[['c', 10], 1]])
    }
  })

  test('supports isIn operator for whereKey', () => {
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
        [['c', 3], 1],
        [['d', 4], 1],
      ]),
    )
    baseInput.sendFrontier(1)

    baseGraph.run()

    const newGraph = new D2({ initialFrontier: 0 })
    const messages: Message<[string, number]>[] = []

    cache
      .pipeInto(newGraph, { whereKey: isIn(['a', 'c', 'e', 'f']) })
      .pipe(output((message) => messages.push(message)))
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
        [['c', 6], 1],
      ])
    }

    // Test incremental updates
    messages.length = 0

    // Send data that matches the isIn operator
    baseInput.sendData(
      2,
      new MultiSet<[string, number]>([
        [['e', 5], 1],
        [['f', 6], 1],
      ]),
    )
    // Send data that doesn't match the isIn operator
    baseInput.sendData(
      2,
      new MultiSet<[string, number]>([
        [['g', 7], 1],
        [['h', 8], 1],
      ]),
    )
    baseInput.sendFrontier(3)

    baseGraph.run()
    newGraph.run()

    expect(messages.length).toEqual(3)
    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([2])
      // Should only include 'a' and 'c' as they're in the set
      expect(data.collection.getInner()).toEqual([
        [['e', 10], 1],
        [['f', 12], 1],
      ])
    }
  })

  test('supports complex operators (and, or, not) for whereKey', () => {
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
        [['d', 4], 1],
        [['e', 5], 1],
      ]),
    )
    baseInput.sendFrontier(1)

    baseGraph.run()

    const newGraph = new D2({ initialFrontier: 0 })
    const messages: Message<[string, number]>[] = []

    // Complex condition: (key > 'b' AND key < 'e') OR key = 'a'
    const complexOperator = or(and(gt('b'), lt('e')), eq('a'))

    cache
      .pipeInto(newGraph, { whereKey: complexOperator })
      .pipe(output((message) => messages.push(message)))
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
        [['d', 8], 1],
      ])
    }

    // Test incremental updates
    messages.length = 0

    // Send data that matches the complex operator
    baseInput.sendData(2, new MultiSet<[string, number]>([[['c', 6], 1]]))
    // Send data that doesn't match the complex operator
    baseInput.sendData(2, new MultiSet<[string, number]>([[['g', 7], 1]]))
    baseInput.sendFrontier(3)

    baseGraph.run()
    newGraph.run()

    expect(messages.length).toEqual(3)
    {
      const msg1 = messages[0]
      expect(msg1.type).toEqual(MessageType.DATA)
      const data = msg1.data as DataMessage<[string, number]>
      expect(data.version.getInner()).toEqual([2])
      // Should only include 'a' and 'c' as they match the complex condition
      expect(data.collection.getInner()).toEqual([[['c', 12], 1]])
    }
  })
})
