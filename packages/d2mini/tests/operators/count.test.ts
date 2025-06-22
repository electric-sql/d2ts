import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { count } from '../../src/operators/count.js'
import { output } from '../../src/operators/output.js'

describe('Operators', () => {
  describe('Count operation', () => {
    testCount()
  })
})

function testCount() {
  test('basic count operation', () => {
    const graph = new D2()
    const input = graph.newInput<[number, string]>()
    const messages: MultiSet<[number, number]>[] = []

    input.pipe(
      count(),
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      new MultiSet([
        [[1, 'a'], 2],
        [[2, 'b'], 1],
        [[2, 'c'], 1],
        [[2, 'd'], 1],
        [[3, 'x'], 1],
        [[3, 'y'], -1],
      ]),
    )
    input.sendData(new MultiSet([[[3, 'z'], 1]]))
    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [[1, 2], 1],
        [[2, 3], 1],
        [[3, 1], 1],
      ],
    ])
  })

  test('count with all negative multiplicities', () => {
    const graph = new D2()
    const input = graph.newInput<[number, string]>()
    const messages: MultiSet<[number, number]>[] = []

    input.pipe(
      count(),
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      new MultiSet([
        [[1, 'a'], -1],
        [[1, 'b'], -2],
      ]),
    )
    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([[[[1, -3], 1]]])
  })

  test('count with multiple batches', () => {
    const graph = new D2()
    const input = graph.newInput<[string, string]>()
    const messages: MultiSet<[string, number]>[] = []

    input.pipe(
      count(),
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      new MultiSet([
        [['one', 'a'], 1],
        [['one', 'b'], 1],
      ]),
    )
    graph.run()

    input.sendData(
      new MultiSet([
        [['one', 'c'], 1],
        [['two', 'a'], 1],
      ]),
    )
    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [[['one', 2], 1]],
      [
        [['one', 2], -1], // <-- old count of 'one' removed
        [['one', 3], 1],
        [['two', 1], 1],
      ],
    ])
  })
}
