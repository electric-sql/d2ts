import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { join, JoinType } from '../../src/operators/join.js'
import { output } from '../../src/operators/output.js'
import { consolidate } from '../../src/operators/consolidate.js'

/**
 * Sort results by multiplicity and then key
 */
function sortResults(results: any[]) {
  return results
    .sort(
      ([[_aKey, _aValue], aMultiplicity], [[_bKey, _bValue], bMultiplicity]) =>
        aMultiplicity - bMultiplicity,
    )
    .sort(
      ([[aKey, _aValue], _aMultiplicity], [[bKey, _bValue], _bMultiplicity]) =>
        aKey - bKey,
    )
}

const joinTypes = ['inner', 'left', 'right', 'full', 'anti'] as const

describe('Operators', () => {
  describe('Join operation', () => {
    joinTypes.forEach((joinType) => {
      describe(`${joinType} join`, () => {
        testJoin(joinType)
      })
    })

    describe('Multiple batch processing regression tests', () => {
      joinTypes.forEach((joinType) => {
        test(`${joinType} join with multiple batches sent before running`, () => {
          const graph = new D2()
          const inputA = graph.newInput<[string, string]>()
          const inputB = graph.newInput<[string, string]>()
          const results: any[] = []

          inputA.pipe(
            join(inputB, joinType as any),
            consolidate(),
            output((message) => {
              results.push(...message.getInner())
            }),
          )

          graph.finalize()

          // Send multiple batches to inputA before running
          inputA.sendData(
            new MultiSet([
              [['batch1_item1', 'a1'], 1],
              [['batch1_item2', 'a2'], 1],
            ]),
          )

          inputA.sendData(new MultiSet([[['batch2_item1', 'a3'], 1]]))

          inputA.sendData(
            new MultiSet([
              [['batch3_item1', 'a4'], 1],
              [['batch3_item2', 'a5'], 1],
            ]),
          )

          // Send corresponding data to inputB (some matches, some don't)
          inputB.sendData(
            new MultiSet([
              [['batch1_item1', 'x1'], 1], // matches
              [['batch2_item1', 'x2'], 1], // matches
              [['batch3_item2', 'x3'], 1], // matches
              [['non_matching', 'x4'], 1], // doesn't match any inputA
            ]),
          )

          // Run the graph - should process all batches
          graph.run()

          // Collect all keys that appear in the results (regardless of multiplicity)
          const processedKeys = new Set<string>()
          for (const [[key, _], _mult] of results) {
            processedKeys.add(key)
          }

          // Verify behavior based on join type
          switch (joinType) {
            case 'inner':
              // Only matching keys should appear
              expect(processedKeys.has('batch1_item1')).toBe(true)
              expect(processedKeys.has('batch2_item1')).toBe(true)
              expect(processedKeys.has('batch3_item2')).toBe(true)
              // Non-matching keys should not appear
              expect(processedKeys.has('batch1_item2')).toBe(false)
              expect(processedKeys.has('batch3_item1')).toBe(false)
              expect(processedKeys.has('non_matching')).toBe(false)
              expect(processedKeys.size).toBe(3)
              break

            case 'left':
              // All inputA keys should appear (some with null for inputB)
              expect(processedKeys.has('batch1_item1')).toBe(true) // matched
              expect(processedKeys.has('batch1_item2')).toBe(true) // unmatched
              expect(processedKeys.has('batch2_item1')).toBe(true) // matched
              expect(processedKeys.has('batch3_item1')).toBe(true) // unmatched
              expect(processedKeys.has('batch3_item2')).toBe(true) // matched
              // InputB-only keys should not appear
              expect(processedKeys.has('non_matching')).toBe(false)
              expect(processedKeys.size).toBe(5)
              break

            case 'right':
              // All inputB keys should appear (some with null for inputA)
              expect(processedKeys.has('batch1_item1')).toBe(true) // matched
              expect(processedKeys.has('batch2_item1')).toBe(true) // matched
              expect(processedKeys.has('batch3_item2')).toBe(true) // matched
              expect(processedKeys.has('non_matching')).toBe(true) // unmatched
              // InputA-only keys should not appear
              expect(processedKeys.has('batch1_item2')).toBe(false)
              expect(processedKeys.has('batch3_item1')).toBe(false)
              expect(processedKeys.size).toBe(4)
              break

            case 'full':
              // All keys from both inputs should appear
              expect(processedKeys.has('batch1_item1')).toBe(true) // matched
              expect(processedKeys.has('batch1_item2')).toBe(true) // inputA only
              expect(processedKeys.has('batch2_item1')).toBe(true) // matched
              expect(processedKeys.has('batch3_item1')).toBe(true) // inputA only
              expect(processedKeys.has('batch3_item2')).toBe(true) // matched
              expect(processedKeys.has('non_matching')).toBe(true) // inputB only
              expect(processedKeys.size).toBe(6)
              break

            case 'anti':
              // Only inputA keys that don't match inputB should appear
              expect(processedKeys.has('batch1_item2')).toBe(true) // unmatched in inputA
              expect(processedKeys.has('batch3_item1')).toBe(true) // unmatched in inputA
              // Matched keys should not appear
              expect(processedKeys.has('batch1_item1')).toBe(false)
              expect(processedKeys.has('batch2_item1')).toBe(false)
              expect(processedKeys.has('batch3_item2')).toBe(false)
              // InputB-only keys should not appear
              expect(processedKeys.has('non_matching')).toBe(false)
              expect(processedKeys.size).toBe(2)
              break
          }

          // Most importantly: ensure we actually got some results
          // (This test would have failed before the bug fix due to data loss)
          expect(results.length).toBeGreaterThan(0)
        })
      })
    })
  })
})

function testJoin(joinType: JoinType) {
  test('initial join with missing rows', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    inputA.sendData(
      new MultiSet([
        [[1, 'A'], 1],
        [[2, 'B'], 1],
      ]),
    )
    inputB.sendData(
      new MultiSet([
        [[2, 'X'], 1],
        [[3, 'Y'], 1],
      ]),
    )
    graph.run()

    const expectedResults = {
      inner: [
        // only 2 is in both streams, so we get it
        [[2, ['B', 'X']], 1],
      ],
      left: [
        // 1 and 2 are in inputA, so we get them
        // 3 is not in inputA, so we don't get it
        [[1, ['A', null]], 1],
        [[2, ['B', 'X']], 1],
      ],
      right: [
        // 2 and 3 are in inputB, so we get them
        // 1 is not in inputB, so we don't get it
        [[2, ['B', 'X']], 1],
        [[3, [null, 'Y']], 1],
      ],
      full: [
        // We get all the rows from both streams
        [[1, ['A', null]], 1],
        [[2, ['B', 'X']], 1],
        [[3, [null, 'Y']], 1],
      ],
      anti: [[[1, ['A', null]], 1]],
    }

    expect(sortResults(results)).toEqual(expectedResults[joinType])
  })

  test('insert left', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(new MultiSet([[[1, 'A'], 1]]))
    inputB.sendData(
      new MultiSet([
        [[1, 'X'], 1],
        [[2, 'Y'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |

        inputB:
        | 1 | X |
        | 2 | Y |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [
        // Only 1 is in both tables, so it's the only result
        [[1, ['A', 'X']], 1],
      ],
      left: [
        // Only 1 is in both tables, so it's the only result
        [[1, ['A', 'X']], 1],
      ],
      right: [
        // 1 is in both so we get it
        [[1, ['A', 'X']], 1],
        // 2 is in inputB, but not in inputA, we get null for inputA
        [[2, [null, 'Y']], 1],
      ],
      full: [
        // 1 is in both so we get it
        [[1, ['A', 'X']], 1],
        // 2 is in inputB, but not in inputA, we get null for inputA
        [[2, [null, 'Y']], 1],
      ],
      anti: [
        // there is nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(
      sortResults(initialExpectedResults[joinType]),
    )

    // Clear results after initial join
    results.length = 0

    // Insert on left side
    inputA.sendData(new MultiSet([[[2, 'B'], 1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 2 | B |

        inputB:
        | 1 | X |
        | 2 | Y |
        */

    const expectedResults = {
      inner: [
        // 2 is now in both tables, so we receive it for the first time
        [[2, ['B', 'Y']], 1],
      ],
      left: [
        // 2 is now in both tables, so we receive it for the first time
        [[2, ['B', 'Y']], 1],
      ],
      right: [
        // we already received 2, but it's updated so we get a -1 and a +1
        // this changes its inputA value from null to B
        [[2, [null, 'Y']], -1],
        [[2, ['B', 'Y']], 1],
      ],
      full: [
        // we already received 2, but it's updated so we get a -1 and a +1
        // this changes its inputA value from null to B
        [[2, [null, 'Y']], -1],
        [[2, ['B', 'Y']], 1],
      ],
      anti: [
        // there is nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(sortResults(expectedResults[joinType]))
  })

  test('insert right', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(
      new MultiSet([
        [[1, 'A'], 1],
        [[3, 'C'], 1],
      ]),
    )
    inputB.sendData(new MultiSet([[[1, 'X'], 1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 3 | C |

        inputB:
        | 1 | X |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [
        // only 1 is in both streams, so we get it
        [[1, ['A', 'X']], 1],
      ],
      left: [
        // only 1 and 3 are in inputA, so we get them
        // 3 is not in inputB, so we get it with null for inputB
        [[1, ['A', 'X']], 1],
        [[3, ['C', null]], 1],
      ],
      right: [
        // only 1 is in inputB, so we get it
        [[1, ['A', 'X']], 1],
      ],
      full: [
        // only 1 is in both streams, so we get it
        [[1, ['A', 'X']], 1],
        // 3 is not in inputB, so we get it with null for inputB
        [[3, ['C', null]], 1],
      ],
      anti: [
        // 3 is unmatched on the left side, so we get it
        [[3, ['C', null]], 1],
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Insert on right side
    inputB.sendData(new MultiSet([[[3, 'Z'], 1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 3 | C |

        inputB:
        | 1 | X |
        | 3 | Z |
        */

    const expectedResults = {
      inner: [
        // 3 is now in both streams, so we get it
        [[3, ['C', 'Z']], 1],
      ],
      left: [
        // 3 is now in inputB, so we get an update chaining null to Z
        [[3, ['C', null]], -1],
        [[3, ['C', 'Z']], 1],
      ],
      right: [
        // 3 is now in inputB, so we now get it
        [[3, ['C', 'Z']], 1],
      ],
      full: [
        // 3 is now in inputB, so we get an update chaining null to Z
        [[3, ['C', null]], -1],
        [[3, ['C', 'Z']], 1],
      ],
      anti: [
        // 3 is now matched on the left side, so it's removed with a -1
        [[3, ['C', null]], -1],
      ],
    }

    expect(sortResults(results)).toEqual(sortResults(expectedResults[joinType]))
  })

  test('delete left', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(
      new MultiSet([
        [[1, 'A'], 1],
        [[2, 'B'], 1],
      ]),
    )
    inputB.sendData(
      new MultiSet([
        [[1, 'X'], 1],
        [[2, 'Y'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 2 | B |

        inputB:
        | 1 | X |
        | 2 | Y |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      left: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      right: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      full: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Delete from left side
    inputA.sendData(new MultiSet([[[1, 'A'], -1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 2 | B |

        inputB:
        | 1 | X |
        | 2 | Y |
        */

    const expectedResults = {
      inner: [
        // 1 was deleted from inputA, so we get a -1
        [[1, ['A', 'X']], -1],
      ],
      left: [
        // 1 was deleted from inputA, so we get a -1
        [[1, ['A', 'X']], -1],
      ],
      right: [
        // 1 was deleted from inputA, so we get an update chaining A to null
        [[1, ['A', 'X']], -1],
        [[1, [null, 'X']], 1],
      ],
      full: [
        // 1 was deleted from inputA, so we get an update chaining A to null
        [[1, ['A', 'X']], -1],
        [[1, [null, 'X']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(expectedResults[joinType])
  })

  test('delete right', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(
      new MultiSet([
        [[1, 'A'], 1],
        [[2, 'B'], 1],
      ]),
    )
    inputB.sendData(
      new MultiSet([
        [[1, 'X'], 1],
        [[2, 'Y'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 2 | B |

        inputB:
        | 1 | X |
        | 2 | Y |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      left: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      right: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      full: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Delete from right side
    inputB.sendData(new MultiSet([[[2, 'Y'], -1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 2 | B |

        inputB:
        | 1 | X |
        */

    const expectedResults = {
      inner: [
        // 2 was deleted from inputB, so we get a -1
        [[2, ['B', 'Y']], -1],
      ],
      left: [
        // 2 was deleted from inputB, we get an update chaining Y to null
        [[2, ['B', 'Y']], -1],
        [[2, ['B', null]], 1],
      ],
      right: [
        // 2 was deleted from inputB, so we get a -1
        [[2, ['B', 'Y']], -1],
      ],
      full: [
        // 2 was deleted from inputB, we get an update chaining Y to null
        [[2, ['B', 'Y']], -1],
        [[2, ['B', null]], 1],
      ],
      anti: [
        // 2 is unmatched on the left side, so we get it
        [[2, ['B', null]], 1],
      ],
    }

    expect(sortResults(results)).toEqual(expectedResults[joinType])
  })

  test('update left (delete + insert)', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(new MultiSet([[[1, 'A'], 1]]))
    inputB.sendData(new MultiSet([[[1, 'X'], 1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |

        inputB:
        | 1 | X |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [[[1, ['A', 'X']], 1]],
      left: [[[1, ['A', 'X']], 1]],
      right: [[[1, ['A', 'X']], 1]],
      full: [[[1, ['A', 'X']], 1]],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Update left (delete + insert)
    inputA.sendData(
      new MultiSet([
        [[1, 'A'], -1],
        [[1, 'A-updated'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A-updated |

        inputB:
        | 1 | X |
        */

    const expectedResults = {
      inner: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      left: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      right: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      full: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(sortResults(expectedResults[joinType]))
  })

  test('update right (delete + insert)', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(new MultiSet([[[1, 'A'], 1]]))
    inputB.sendData(new MultiSet([[[1, 'X'], 1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |

        inputB:
        | 1 | X |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [[[1, ['A', 'X']], 1]],
      left: [[[1, ['A', 'X']], 1]],
      right: [[[1, ['A', 'X']], 1]],
      full: [[[1, ['A', 'X']], 1]],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Update right (delete + insert)
    inputB.sendData(
      new MultiSet([
        [[1, 'X'], -1],
        [[1, 'X-updated'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |

        inputB:
        | 1 | X-updated |
        */

    const expectedResults = {
      inner: [
        // 1 was already in both streams, so we get an update chaining X to X-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A', 'X-updated']], 1],
      ],
      left: [
        // 1 was already in both streams, so we get an update chaining X to X-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A', 'X-updated']], 1],
      ],
      right: [
        // 1 was already in both streams, so we get an update chaining X to X-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A', 'X-updated']], 1],
      ],
      full: [
        // 1 was already in both streams, so we get an update chaining X to X-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A', 'X-updated']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(sortResults(expectedResults[joinType]))
  })

  test('delete both', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      // When we delete from both side, we can get extra updates within the
      // same batch that all cancel out. This consolidates them into a single
      // update with the net change.
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(
      new MultiSet([
        [[1, 'A'], 1],
        [[2, 'B'], 1],
      ]),
    )
    inputB.sendData(
      new MultiSet([
        [[1, 'X'], 1],
        [[2, 'Y'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 2 | B |

        inputB:
        | 1 | X |
        | 2 | Y |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      left: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      right: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      full: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Delete from both sides
    inputA.sendData(new MultiSet([[[1, 'A'], -1]]))
    inputB.sendData(new MultiSet([[[1, 'X'], -1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 2 | B |

        inputB:
        | 2 | Y |
        */

    const expectedResults = {
      // 1 was deleted from both streams, so we get a -1 on all of them
      inner: [[[1, ['A', 'X']], -1]],
      left: [[[1, ['A', 'X']], -1]],
      right: [[[1, ['A', 'X']], -1]],
      full: [[[1, ['A', 'X']], -1]],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(expectedResults[joinType])
  })

  test('update one then delete both', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      // When we delete from both side, we can get extra updates within the
      // same batch that all cancel out. This consolidates them into a single
      // update with the net change.
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(
      new MultiSet([
        [[1, 'A'], 1],
        [[2, 'B'], 1],
      ]),
    )
    inputB.sendData(
      new MultiSet([
        [[1, 'X'], 1],
        [[2, 'Y'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 2 | B |

        inputB:
        | 1 | X |
        | 2 | Y |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      left: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      right: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      full: [
        [[1, ['A', 'X']], 1],
        [[2, ['B', 'Y']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Update left (delete + insert)
    inputA.sendData(
      new MultiSet([
        [[1, 'A'], -1],
        [[1, 'A-updated'], 1],
      ]),
    )
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A-updated |

        inputB:
        | 1 | X |
        */

    const expectedResults2 = {
      inner: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      left: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      right: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      full: [
        // 1 was already in both streams, so we get an update chaining A to A-updated
        [[1, ['A', 'X']], -1],
        [[1, ['A-updated', 'X']], 1],
      ],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(
      sortResults(expectedResults2[joinType]),
    )

    results.length = 0

    // Delete from both sides
    inputA.sendData(new MultiSet([[[1, 'A-updated'], -1]]))
    inputB.sendData(new MultiSet([[[1, 'X'], -1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 2 | B |

        inputB:
        | 2 | Y |
        */

    const expectedResults = {
      // 1 was deleted from both streams, so we get a -1 on all of them
      inner: [[[1, ['A-updated', 'X']], -1]],
      left: [[[1, ['A-updated', 'X']], -1]],
      right: [[[1, ['A-updated', 'X']], -1]],
      full: [[[1, ['A-updated', 'X']], -1]],
      anti: [
        // nothing unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(expectedResults[joinType])
  })

  test('insert both', () => {
    const graph = new D2()
    const inputA = graph.newInput<[number, string]>()
    const inputB = graph.newInput<[number, string]>()
    const results: any[] = []

    inputA.pipe(
      join(inputB, joinType as any),
      consolidate(),
      output((message) => {
        results.push(...message.getInner())
      }),
    )

    graph.finalize()

    // Initial data
    inputA.sendData(new MultiSet([[[1, 'A'], 1]]))
    inputB.sendData(new MultiSet([[[2, 'Y'], 1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |

        inputB:
        | 2 | Y |
        */

    // Check initial state
    const initialExpectedResults = {
      inner: [
        // Nothing in both streams
      ],
      left: [
        // We get a null for inputB because 1 is not in inputB
        [[1, ['A', null]], 1],
      ],
      right: [
        // We get a null for inputA because 1 is not in inputA
        [[2, [null, 'Y']], 1],
      ],
      full: [
        // We get a null for inputB because 1 is not in inputB
        [[1, ['A', null]], 1],
        // We get a null for inputA because 1 is not in inputA
        [[2, [null, 'Y']], 1],
      ],
      anti: [
        // 1 is unmatched on the left side, so we get it
        [[1, ['A', null]], 1],
      ],
    }

    expect(sortResults(results)).toEqual(initialExpectedResults[joinType])

    // Clear results after initial join
    results.length = 0

    // Insert on both sides with matching key
    inputA.sendData(new MultiSet([[[3, 'C'], 1]]))
    inputB.sendData(new MultiSet([[[3, 'Z'], 1]]))
    graph.run()

    /*
        As tables:
        inputA:
        | 1 | A |
        | 3 | C |

        inputB:
        | 2 | Y |
        | 3 | Z |
        */

    const expectedResults = {
      // 3 is new in both streams, so we get it
      inner: [[[3, ['C', 'Z']], 1]],
      left: [[[3, ['C', 'Z']], 1]],
      right: [[[3, ['C', 'Z']], 1]],
      full: [[[3, ['C', 'Z']], 1]],
      anti: [
        // nothing new is unmatched on the left side, so we get nothing
      ],
    }

    expect(sortResults(results)).toEqual(expectedResults[joinType])
  })
}
