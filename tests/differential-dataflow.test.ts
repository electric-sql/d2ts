import { describe, test, expect, vi } from 'vitest'
import { DifferenceStreamBuilder, GraphBuilder } from '../src/builder'
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

  test('basic consolidate operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [input, writer] = graphBuilder.newInput<number>()

    let messages: DataMessage<number>[] = []

    const output = input.consolidate().output((message) => {
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
      ]),
    )
    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [3, 1],
        [4, 1],
      ]),
    )
    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [3, 2],
        [2, -1],
      ]),
    ) // Add two more 3, remove 2
    writer.sendFrontier(new Antichain([v([1, 1])])) // <-- TODO: Is this correct?

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [1, 1],
        [3, 3],
        [4, 1],
      ],
    ])
  })

  test('basic join operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [inputA, writerA] = graphBuilder.newInput<[number, string]>()
    const [inputB, writerB] = graphBuilder.newInput<[number, string]>()

    let messages: DataMessage<[number, [string, string]]>[] = []

    const output = inputA.join(inputB).output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writerA.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )
    writerA.sendFrontier(new Antichain([v([1, 0])]))
    writerB.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
        [[3, 'z'], 1],
      ]),
    )
    writerB.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])

    messages = []

    writerA.sendData(v([2, 0]), new MultiSet([[[3, 'c'], 1]]))
    writerA.sendFrontier(new Antichain([v([2, 0])]))

    graph.step()

    const data2 = messages.map((m) => m.collection.getInner())

    expect(data2).toEqual([[[[3, ['c', 'z']], 1]]])
  })

  test('basic reduce operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [input, writer] = graphBuilder.newInput<[string, number]>()

    let messages: DataMessage<[string, number]>[] = []

    const output = input
      .reduce((vals) => {
        let sum = 0
        for (const [val, diff] of vals) {
          sum += val * diff
        }
        return [[sum, 1]]
      })
      .output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [['a', 1], 2],
        [['a', 2], 1],
        [['a', 3], 1],
        [['b', 4], 1],
      ]),
    )
    writer.sendData(v([1, 0]), new MultiSet([[['b', 5], 1]]))
    writer.sendFrontier(new Antichain([v([2, 0])])) // <-- TODO: Is this correct?

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [['a', 7], 1],
        [['b', 9], 1],
      ],
    ])
  })

  test('basic count operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [input, writer] = graphBuilder.newInput<[number, string]>()

    let messages: DataMessage<[number, number]>[] = []

    const output = input.count().output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 2],
        [[2, 'b'], 1],
        [[2, 'c'], 1],
        [[2, 'd'], 1],
        [[3, 'x'], 1],
        [[3, 'y'], -1],
      ]),
    )
    writer.sendData(v([1, 0]), new MultiSet([[[3, 'z'], 1]]))
    writer.sendFrontier(new Antichain([v([1, 1])])) // <-- TODO: Is this correct?

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, 2], 1],
        [[2, 3], 1],
        [[3, 1], 1],
      ],
    ])
  })

  test('basic distinct operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

    const [input, writer] = graphBuilder.newInput<[number, string]>()

    let messages: DataMessage<[number, string]>[] = []

    const output = input.distinct().output((message) => {
      if (message.type === MessageType.DATA) {
        messages.push(message.data)
      }
    })

    const graph = graphBuilder.finalize()

    writer.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 2],
        [[2, 'b'], 1],
        [[2, 'c'], 2],
      ]),
    )
    writer.sendFrontier(new Antichain([v([1, 1])])) // <-- TODO: Is this correct?

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, 'a'], 1],
        [[2, 'b'], 1],
        [[2, 'c'], 1],
      ],
    ])
  })

  test('iterate operation', () => {
    const graphBuilder = new GraphBuilder(new Antichain([v(0)]))

    const [input_a, writer_a] = graphBuilder.newInput<number>()

    const geometricSeries = (
      stream: DifferenceStreamBuilder<number>,
    ): DifferenceStreamBuilder<number> => {
      return stream
        .map((x) => x * 2)
        .concat(stream)
        .filter((x) => x <= 50)
        .map((x) => [x, []])
        .distinct()
        .map((x) => x[0])
        .consolidate() as DifferenceStreamBuilder<number>
    }

    const output = input_a.iterate(geometricSeries).connectReader()
    const graph = graphBuilder.finalize()

    writer_a.sendData(v(0), new MultiSet([[1, 1]]))
    writer_a.sendFrontier(new Antichain([v(1)]))

    while (output.probeFrontierLessThan(new Antichain([v(1)]))) {
      graph.step()
    }

    const data = output
      .drain()
      .filter((m) => m.type === MessageType.DATA)
      .map((m) => m.data.collection.getInner())

    expect(data).toEqual([
      [
        [1, 1],
        [2, 1],
      ],
      [[4, 1]],
      [[8, 1]],
      [[16, 1]],
      [[32, 1]],
    ])
  })
})
