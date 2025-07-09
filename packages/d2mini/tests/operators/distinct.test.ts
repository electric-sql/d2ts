import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { distinct } from '../../src/operators/distinct.js'
import { output } from '../../src/operators/output.js'

describe('Operators', () => {
  describe('Efficient distinct operation', () => {
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

  test('distinct by certain property', () => {
    const graph = new D2()
    const input = graph.newInput<[number, { name: string; country: string }]>()
    const messages: MultiSet<[number, { name: string; country: string }]>[] = []

    input.pipe(
      distinct(([_, value]) => value.country),
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      new MultiSet([
        [[1, { name: 'Valter', country: 'Portugal' }], 1],
        [[2, { name: 'Sam', country: 'UK' }], 1],
        [[2, { name: 'Kevin', country: 'Belgium' }], 1],
        [[3, { name: 'Garry', country: 'UK' }], 1],
        [[4, { name: 'Kyle', country: 'USA' }], 1],
      ]),
    )

    graph.run()

    const data = messages.map((m) => m.getInner())[0]
    const countries = data
      .map(([[_, value], multiplicity]) => [value.country, multiplicity])
      .sort()

    expect(countries).toEqual(
      [
        ['Belgium', 1],
        ['Portugal', 1],
        ['UK', 1],
        ['USA', 1],
      ].sort(),
    )
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
        [[1, 'a'], 1],
      ]),
    )
    graph.run()

    input.sendData(
      new MultiSet([
        [[1, 'b'], -1],
        [[1, 'c'], 2],
        [[1, 'a'], -1],
      ]),
    )
    graph.run()

    input.sendData(new MultiSet([[[1, 'c'], -2]]))
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
      [[[1, 'c'], -1]],
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

  test('distinct with multiple batches of same key that cancel out', () => {
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
        [['key1', 2], -3], // cancels out the previous addition of [['key2', 2], 3]
        [['key2', 1], 1],
      ]),
    )
    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [['key1', 1], 1],
        [['key2', 1], 1],
      ],
    ])
  })
}
