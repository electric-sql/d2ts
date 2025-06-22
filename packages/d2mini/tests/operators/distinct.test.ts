import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { distinct } from '../../src/operators/distinct.js'
import { output } from '../../src/operators/output.js'

describe('Operators', () => {
  describe('Distinct operation', () => {
    testDistinct()
  })
})

function testDistinct() {
  test('basic distinct operation', () => {
    const graph = new D2()
    const input = graph.newInput<[number, string]>()
    const messages: MultiSet<[number, string]>[] = []

    input.pipe(
      distinct(),
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      new MultiSet([
        [[1, 'a'], 2],
        [[2, 'b'], 1],
        [[2, 'c'], 2],
      ]),
    )
    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [[1, 'a'], 1],
        [[2, 'b'], 1],
        [[2, 'c'], 1],
      ],
    ])
  })

  test('distinct with updates', () => {
    const graph = new D2()
    const input = graph.newInput<[number, string]>()
    const messages: MultiSet<[number, string]>[] = []

    input.pipe(
      distinct(),
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      new MultiSet([
        [[1, 'a'], 1],
        [[1, 'b'], 1],
      ]),
    )
    graph.run()

    input.sendData(
      new MultiSet([
        [[1, 'b'], -1],
        [[1, 'c'], 1],
      ]),
    )
    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [[1, 'a'], 1],
        [[1, 'b'], 1],
      ],
      [
        [[1, 'b'], -1],
        [[1, 'c'], 1],
      ],
    ])
  })

  test('distinct with multiple batches of same key', () => {
    const graph = new D2()
    const input = graph.newInput<[string, number]>()
    const messages: MultiSet<[string, number]>[] = []

    input.pipe(
      distinct(),
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      new MultiSet([
        [['key1', 1], 2],
        [['key1', 2], 3],
        [['key2', 1], 1],
      ]),
    )
    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [['key1', 1], 1],
        [['key1', 2], 1],
        [['key2', 1], 1],
      ],
    ])
  })
}
