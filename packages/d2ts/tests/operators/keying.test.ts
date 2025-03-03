import { describe, it, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { keyBy, unkey, rekey } from '../../src/operators/keying.js'
import { output } from '../../src/operators/index.js'
import { MultiSet } from '../../src/multiset.js'
import { Message, MessageType } from '../../src/types.js'
import { Antichain, v } from '../../src/order.js'

interface TestItem {
  id: number
  name: string
  value: number
}

describe('keying operators', () => {
  it('should key a stream by a property', () => {
    const d2 = new D2({ initialFrontier: v([0, 0]) })
    const input = d2.newInput<TestItem>()
    const messages: Message<TestItem>[] = []

    const keyed = input.pipe(keyBy((item) => item.id))
    const outputStream = keyed.pipe(unkey())
    outputStream.pipe(output((message) => messages.push(message)))

    input.sendData(
      v([1, 0]),
      new MultiSet([[{ id: 1, name: 'a', value: 10 }, 1]]),
    )
    input.sendData(
      v([2, 0]),
      new MultiSet([[{ id: 2, name: 'b', value: 20 }, 1]]),
    )
    input.sendFrontier(new Antichain([v([2, 0])]))
    d2.finalize()
    d2.run()

    expect(messages).toEqual([
      {
        type: MessageType.DATA,
        data: {
          version: v([1, 0]),
          collection: new MultiSet([[{ id: 1, name: 'a', value: 10 }, 1]]),
        },
      },
      {
        type: MessageType.DATA,
        data: {
          version: v([2, 0]),
          collection: new MultiSet([[{ id: 2, name: 'b', value: 20 }, 1]]),
        },
      },
      { type: MessageType.FRONTIER, data: new Antichain([v([2, 0])]) },
    ])
  })

  it('should rekey a stream with new keys', () => {
    const d2 = new D2({ initialFrontier: v([0, 0]) })
    const input = d2.newInput<TestItem>()
    const messages: Message<TestItem>[] = []

    // First key by id
    const keyed = input.pipe(keyBy((item) => item.id))
    // Then rekey by name
    const rekeyed = keyed.pipe(rekey((item) => item.name))
    const outputStream = rekeyed.pipe(unkey())
    outputStream.pipe(output((message) => messages.push(message)))

    input.sendData(
      v([1, 0]),
      new MultiSet([[{ id: 1, name: 'a', value: 10 }, 1]]),
    )
    input.sendData(
      v([2, 0]),
      new MultiSet([[{ id: 2, name: 'b', value: 20 }, 1]]),
    )
    input.sendFrontier(new Antichain([v([2, 0])]))
    d2.finalize()
    d2.run()

    expect(messages).toEqual([
      {
        type: MessageType.DATA,
        data: {
          version: v([1, 0]),
          collection: new MultiSet([[{ id: 1, name: 'a', value: 10 }, 1]]),
        },
      },
      {
        type: MessageType.DATA,
        data: {
          version: v([2, 0]),
          collection: new MultiSet([[{ id: 2, name: 'b', value: 20 }, 1]]),
        },
      },
      { type: MessageType.FRONTIER, data: new Antichain([v([2, 0])]) },
    ])
  })

  it('should handle multiple updates to the same key', () => {
    const d2 = new D2({ initialFrontier: v([0, 0]) })
    const input = d2.newInput<TestItem>()
    const messages: Message<TestItem>[] = []

    const keyed = input.pipe(keyBy((item) => item.id))
    const outputStream = keyed.pipe(unkey())
    outputStream.pipe(output((message) => messages.push(message)))

    input.sendData(
      v([1, 0]),
      new MultiSet([[{ id: 1, name: 'a', value: 10 }, 1]]),
    )
    input.sendData(
      v([2, 0]),
      new MultiSet([[{ id: 1, name: 'a', value: 20 }, 1]]),
    )
    input.sendFrontier(new Antichain([v([2, 0])]))
    d2.finalize()
    d2.run()

    expect(messages).toEqual([
      {
        type: MessageType.DATA,
        data: {
          version: v([1, 0]),
          collection: new MultiSet([[{ id: 1, name: 'a', value: 10 }, 1]]),
        },
      },
      {
        type: MessageType.DATA,
        data: {
          version: v([2, 0]),
          collection: new MultiSet([[{ id: 1, name: 'a', value: 20 }, 1]]),
        },
      },
      { type: MessageType.FRONTIER, data: new Antichain([v([2, 0])]) },
    ])
  })
})
