import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/pipe'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'
import { DataMessage, MessageType } from '../../src/types'
import { join, output } from '../../src/operators'
import Database from 'better-sqlite3'

describe('Operators - in-memory', () => {
  describe('Join operation', () => {
    testJoin()
  })
})

describe('Operators - sqlite', () => {
  const newDb = () => new Database(':memory:')
  describe('Join operation', () => {
    testJoin(newDb)
  })
})

function testJoin(newDb?: () => InstanceType<typeof Database>) {
  test('basic join operation', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    let messages: DataMessage<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })
    )

    graph.finalize()

    inputA.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))
    
    inputB.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
        [[3, 'z'], 1],
      ]),
    )
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])
  })

  test('join with late arriving data', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    let messages: DataMessage<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })
    )

    graph.finalize()

    inputA.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    inputB.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
      ]),
    )
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], 1],
      ],
    ])
  })

  test('join with negative multiplicities', () => {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    let messages: DataMessage<[number, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        if (message.type === MessageType.DATA) {
          messages.push(message.data)
        }
      })
    )

    graph.finalize()

    inputA.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], -1],
      ]),
    )
    inputB.sendData(
      v([1, 0]),
      new MultiSet([
        [[1, 'x'], 1],
        [[2, 'y'], 1],
      ]),
    )
    inputA.sendFrontier(new Antichain([v([1, 0])]))
    inputB.sendFrontier(new Antichain([v([1, 0])]))

    graph.step()

    const data = messages.map((m) => m.collection.getInner())

    expect(data).toEqual([
      [
        [[1, ['a', 'x']], 1],
        [[2, ['b', 'y']], -1],
      ],
    ])
  })
}
