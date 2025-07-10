import { describe, test, expect } from 'vitest'
import { D2 } from '../src/d2.js'
import { MultiSet, LazyMultiSet } from '../src/multiset.js'
import { map, filter, negate, output } from '../src/operators/index.js'

describe('Lazy Evaluation Demo', () => {
  test('chained operations with lazy evaluation', () => {
    const graph = new D2()
    const input = graph.newInput<number>()
    const messages: any[] = []

    // Create a pipeline with multiple chained operations
    input.pipe(
      map((x) => x * 2),        // Double each number
      filter((x) => x > 4),     // Keep only numbers > 4  
      map((x) => x + 1),        // Add 1 to each
      negate(),                 // Negate multiplicities
      output((message) => {
        messages.push(message.getInner())
      }),
    )

    graph.finalize()

    // Input some data
    input.sendData(
      new MultiSet([
        [1, 1],   // 1 * 2 = 2, filtered out (2 <= 4)
        [2, 2],   // 2 * 2 = 4, filtered out (4 <= 4)  
        [3, 1],   // 3 * 2 = 6, kept, +1 = 7, negated = [7, -1]
        [4, 1],   // 4 * 2 = 8, kept, +1 = 9, negated = [9, -1]
        [5, 2],   // 5 * 2 = 10, kept, +1 = 11, negated = [11, -2]
      ]),
    )

    graph.run()

    // The lazy evaluation means each operator processes items as they're needed
    // rather than materializing intermediate results
    expect(messages).toEqual([
      [
        [7, -1],   // from input 3
        [9, -1],   // from input 4
        [11, -2],  // from input 5
      ],
    ])
  })

  test('lazy multiset can be iterated without full materialization', () => {
    // Create a large dataset
    const largeData: [number, number][] = []
    for (let i = 0; i < 1000; i++) {
      largeData.push([i, 1])
    }

    const lazySet = LazyMultiSet.fromArray(largeData)
      .filter((x) => x % 100 === 0)  // Keep only multiples of 100
      .map((x) => x * 2)             // Double them

    // We can iterate over just the first few results without processing all 1000 items
    const firstThree: [number, number][] = []
    let count = 0
    for (const [value, mult] of lazySet) {
      if (count >= 3) break
      firstThree.push([value, mult])
      count++
    }

    expect(firstThree).toEqual([
      [0, 1],     // 0 * 2 = 0
      [200, 1],   // 100 * 2 = 200
      [400, 1],   // 200 * 2 = 400
    ])
  })

  test('compare memory usage: eager vs lazy', () => {
    // This test demonstrates the concept - in practice, lazy evaluation
    // would use less memory for large datasets with filtering
    
    const data: [number, number][] = []
    for (let i = 0; i < 100; i++) {
      data.push([i, 1])
    }

    // Eager evaluation (traditional MultiSet)
    const eager = new MultiSet(data)
      .map((x) => x * 2)
      .filter((x) => x > 150)  // This would create intermediate arrays
      .map((x) => x + 10)

    // Lazy evaluation (LazyMultiSet)
    const lazy = LazyMultiSet.fromArray(data)
      .map((x) => x * 2)
      .filter((x) => x > 150)  // This creates generators, not arrays
      .map((x) => x + 10)

    // Both should produce the same result
    expect(lazy.getInner()).toEqual(eager.getInner())
    
    // But the lazy version processes items on-demand rather than
    // creating intermediate collections
  })
})