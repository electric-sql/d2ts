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

  test('join with multiple batches sent before running - regression test for data loss bug', () => {
    const graph = new D2()
    const inputA = graph.newInput<[string, string]>()
    const inputB = graph.newInput<[string, string]>()
    const messages: MultiSet<[string, [string, string]]>[] = []

    inputA.pipe(
      join(inputB),
      output((message) => {
        messages.push(message as MultiSet<[string, [string, string]]>)
      }),
    )

    graph.finalize()

    // Send multiple batches to inputA before running
    inputA.sendData(
      new MultiSet([
        [['key1', 'batch1_a'], 1],
        [['key2', 'batch1_b'], 1],
      ]),
    )

    inputA.sendData(
      new MultiSet([
        [['key3', 'batch2_a'], 1],
        [['key4', 'batch2_b'], 1],
      ]),
    )

    inputA.sendData(new MultiSet([[['key5', 'batch3_a'], 1]]))

    // Send corresponding data to inputB
    inputB.sendData(
      new MultiSet([
        [['key1', 'x1'], 1],
        [['key2', 'x2'], 1],
        [['key3', 'x3'], 1],
        [['key4', 'x4'], 1],
        [['key5', 'x5'], 1],
      ]),
    )

    // Run the graph - should process all batches
    graph.run()

    // Verify we got results
    expect(messages.length).toBeGreaterThan(0)

    // Collect all keys that were processed
    const processedKeys = new Set<string>()
    for (const message of messages) {
      for (const [[key, _], _mult] of message.getInner()) {
        processedKeys.add(key)
      }
    }

    // All keys from all batches should be present
    const expectedKeys = ['key1', 'key2', 'key3', 'key4', 'key5']
    for (const key of expectedKeys) {
      expect(processedKeys.has(key)).toBe(true)
    }

    expect(processedKeys.size).toBe(5)
  })

  test('join comparison: step-by-step vs batch processing should give same results', () => {
    // Step-by-step processing
    const graph1 = new D2()
    const inputA1 = graph1.newInput<[string, string]>()
    const inputB1 = graph1.newInput<[string, string]>()
    const stepMessages: MultiSet<any>[] = []

    inputA1.pipe(
      join(inputB1),
      output((message) => {
        stepMessages.push(message)
      }),
    )

    graph1.finalize()

    // Set up inputB data first
    inputB1.sendData(
      new MultiSet([
        [['item1', 'x1'], 1],
        [['item2', 'x2'], 1],
        [['item3', 'x3'], 1],
      ]),
    )

    // Send and process inputA one batch at a time
    inputA1.sendData(new MultiSet([[['item1', 'a1'], 1]]))
    graph1.run()

    inputA1.sendData(new MultiSet([[['item2', 'a2'], 1]]))
    graph1.run()

    inputA1.sendData(new MultiSet([[['item3', 'a3'], 1]]))
    graph1.run()

    // Batch processing
    const graph2 = new D2()
    const inputA2 = graph2.newInput<[string, string]>()
    const inputB2 = graph2.newInput<[string, string]>()
    const batchMessages: MultiSet<any>[] = []

    inputA2.pipe(
      join(inputB2),
      output((message) => {
        batchMessages.push(message)
      }),
    )

    graph2.finalize()

    // Set up inputB data
    inputB2.sendData(
      new MultiSet([
        [['item1', 'x1'], 1],
        [['item2', 'x2'], 1],
        [['item3', 'x3'], 1],
      ]),
    )

    // Send all inputA batches then run once
    inputA2.sendData(new MultiSet([[['item1', 'a1'], 1]]))
    inputA2.sendData(new MultiSet([[['item2', 'a2'], 1]]))
    inputA2.sendData(new MultiSet([[['item3', 'a3'], 1]]))
    graph2.run()

    // Collect all keys from both approaches
    const stepKeys = new Set<string>()
    const batchKeys = new Set<string>()

    for (const message of stepMessages) {
      for (const [[key, _], _mult] of message.getInner()) {
        stepKeys.add(key)
      }
    }

    for (const message of batchMessages) {
      for (const [[key, _], _mult] of message.getInner()) {
        batchKeys.add(key)
      }
    }

    // Both approaches should process the same items
    expect(stepKeys.size).toBe(3)
    expect(batchKeys.size).toBe(3)
    expect(stepKeys).toEqual(batchKeys)
  })
}
