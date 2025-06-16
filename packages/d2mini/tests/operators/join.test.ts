import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { join } from '../../src/operators/join.js'
import { output } from '../../src/operators/output.js'

describe('Operators', () => {
  describe('Join operation', () => {
    testJoin()
  })
})

function testJoin() {
  test('basic join operation', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: MultiSet<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        messages.push(message as MultiSet<[number, [string, string]]>)
      }),
    )

    graph.finalize()

    inputA.sendData(
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )

    inputB.sendData(
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
        [[3, 'z'], 1],
      ]),
    )

    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])
  })

  test('join with late arriving data', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: MultiSet<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        messages.push(message as MultiSet<[number, [string, string]]>)
      }),
    )

    graph.finalize()

    inputA.sendData(
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )

    graph.run()

    inputB.sendData(
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
      ]),
    )

    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])
  })

  test('join with negative multiplicities', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const messages: MultiSet<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        messages.push(message as MultiSet<[number, [string, string]]>)
      }),
    )

    graph.finalize()

    inputA.sendData(
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], -1],
      ]),
    )
    inputB.sendData(
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
      ]),
    )

    graph.run()

    const data = messages.map((m) => m.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], -1],
      ],
    ])
  })
}
