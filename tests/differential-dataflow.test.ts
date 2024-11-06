import { describe, test, expect, vi } from 'vitest'
import { GraphBuilder } from '../src/builder'
import { MultiSet } from '../src/multiset'
import { Antichain, v } from '../src/order'
import { DataMessage, Message, MessageType } from '../src/types'

describe('Differential dataflow', () => {
  test('basic map operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
    const [input, writer] = graphBuilder.newInput<number>()

    let messages: Message<number>[] = []

    // Create a simple pipeline that adds 5 and filters even numbers
    const output = input
      .map((x) => x + 5)
      .output((message) => {
        messages.push(message)
      })

    const graph = graphBuilder.finalize()

    // Send initial data
    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [1, 1],
        [2, 1],
        [3, 1],
      ]),
    )
    writer.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    expect(messages).toEqual([
      {
        type: MessageType.DATA,
        data: {
          version: v([1, 0]),
          collection: new MultiSet([
            [6, 1],
            [7, 1],
            [8, 1],
          ]),
        },
      },
      { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
    ])
  })

  test('basic filter operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
    const [input, writer] = graphBuilder.newInput<number>()

    let messages: Message<number>[] = []

    const output = input
      .filter((x) => x % 2 === 0)
      .output((message) => {
        messages.push(message)
      })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [1, 1],
        [2, 1],
        [3, 1],
      ]),
    )
    writer.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    expect(messages).toEqual([
      {
        type: MessageType.DATA,
        data: { version: v([1, 0]), collection: new MultiSet([[2, 1]]) },
      },
      { type: MessageType.FRONTIER, data: new Antichain([v([1, 0])]) },
    ])
  })

  test('basic negate operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
    const [input, writer] = graphBuilder.newInput<number>()

    let messages: DataMessage<number>[] = []

    const output = input.negate().output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [1, 1],
        [2, 1],
        [3, 1],
      ]),
    )
    writer.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [1, -1],
        [2, -1],
        [3, -1],
      ],
    ])
  })

  test('basic concat operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [inputA, writerA] = graphBuilder.newInput<number>()
    const [inputB, writerB] = graphBuilder.newInput<number>()

    let messages: DataMessage<number>[] = []

    const output = inputA.concat(inputB).output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writerA.sendData(
      v([1, 0]),
      new MultiSet([
        [1, 1],
        [2, 1],
      ]),
    )
    writerA.sendFrontier(new Antichain([v([1, 0])]))
    writerB.sendData(
      v([1, 0]),
      new MultiSet([
        [3, 1],
        [4, 1],
      ]),
    )
    writerB.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [1, 1],
        [2, 1],
      ],
      [
        [3, 1],
        [4, 1],
      ],
    ])
  })

  test('basic debug operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [input, writer] = graphBuilder.newInput<number>()

    const output = input.debug('test')

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [1, 1],
        [2, 1],
      ]),
    )
    writer.sendFrontier(new Antichain([v([1, 0])]))

    const consoleSpy = vi.spyOn(console, 'log')
    graph.step()
    expect(consoleSpy).toHaveBeenCalledWith(
      `debug test data: version: Version([1,0]) collection: MultiSet([[1,1],[2,1]])`,
    )
    expect(consoleSpy).toHaveBeenCalledWith(
      `debug test notification: frontier Antichain([[1,0]])`,
    )
    consoleSpy.mockRestore()
  })

  test('basic output operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [input, writer] = graphBuilder.newInput<number>()

    let messages: DataMessage<number>[] = []

    const output = input.output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [1, 1],
        [2, 1],
        [3, 1],
      ]),
    )
    writer.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [1, 1],
        [2, 1],
        [3, 1],
      ],
    ])
  })
})
