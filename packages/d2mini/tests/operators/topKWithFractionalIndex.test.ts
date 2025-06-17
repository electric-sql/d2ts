import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { topKWithFractionalIndex } from '../../src/operators/topKWithFractionalIndex.js'
import { output } from '../../src/operators/index.js'

// Helper function to check if indices are in lexicographic order
function checkLexicographicOrder(results: any[]) {
  // Extract values and their indices
  const valuesWithIndices = results.map(([[_, [value, index]]]) => ({
    value,
    index,
  }))

  // Sort by value using the same comparator as in the test
  const sortedByValue = [...valuesWithIndices].sort((a, b) =>
    a.value.value < b.value.value ? -1 : 1,
  )

  // Check that indices are in the same order as the sorted values
  for (let i = 0; i < sortedByValue.length - 1; i++) {
    const currentIndex = sortedByValue[i].index
    const nextIndex = sortedByValue[i + 1].index

    // Indices should be in lexicographic order
    expect(currentIndex < nextIndex).toBe(true)
  }

  return true
}

// Helper function to verify the expected order of elements
function verifyOrder(results: any[], expectedOrder: string[]) {
  // Extract values in the order they appear in the results
  const actualOrder = results.map(([[_, [value, __]]]) => value.value)

  // Sort both arrays to ensure consistent comparison
  const sortedActual = [...actualOrder].sort()
  const sortedExpected = [...expectedOrder].sort()

  // First check that we have the same elements
  expect(sortedActual).toEqual(sortedExpected)

  // Now check that the indices result in the correct order
  const valueToIndex = new Map()
  for (const [[_, [value, index]]] of results) {
    valueToIndex.set(value.value, index)
  }

  // Sort the values by their indices
  const sortedByIndex = [...valueToIndex.entries()]
    .sort((a, b) => (a[1] < b[1] ? -1 : 1))
    .map(([value]) => value)

  // The order should match the expected order
  expect(sortedByIndex).toEqual(expectedOrder)
}

describe('Operators', () => {
  describe('TopKWithFractionalIndex operation', () => {
    it('should assign fractional indices to sorted elements', () => {
      const graph = new D2()
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithFractionalIndex((a, b) => a.value.localeCompare(b.value)),
        output((message) => {
          allMessages.push(message)
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      graph.run()

      // Initial result should have all elements with fractional indices
      const initialResult = allMessages[0].getInner()
      expect(initialResult.length).toBe(5)

      // Check that indices are in lexicographic order
      expect(checkLexicographicOrder(initialResult)).toBe(true)

      // Store the initial indices for later comparison
      const initialIndices = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        initialIndices.set(value.id, index)
      }

      // Now let's move 'c' to the beginning by changing its value
      input.sendData(
        new MultiSet([
          [[null, { id: 3, value: 'a-' }], 1], // This should now be first
          [[null, { id: 3, value: 'c' }], -1], // Remove the old value
        ]),
      )
      graph.run()

      // Check the changes
      const changes = allMessages[1].getInner()

      // We should only emit as many changes as we received
      // We received 2 changes (1 addition, 1 removal)
      // We should emit at most 2 changes
      expect(changes.length).toBeLessThanOrEqual(2)
      expect(changes.length).toBe(2) // 1 removal + 1 addition

      // Find the removal and addition
      const removal = changes.find(([_, multiplicity]) => multiplicity < 0)
      const addition = changes.find(([_, multiplicity]) => multiplicity > 0)

      // Check that we removed 'c' and added 'a-'
      expect(removal?.[0][1][0].value).toBe('c')
      expect(addition?.[0][1][0].value).toBe('a-')

      // Check that the id is the same (id 3)
      expect(removal?.[0][1][0].id).toBe(3)
      expect(addition?.[0][1][0].id).toBe(3)

      // Get the new index
      const newIndex = addition?.[0][1][1]
      const oldIndex = removal?.[0][1][1]

      // The new index should be different from the old one
      expect(newIndex).not.toBe(oldIndex)

      // Reconstruct the current state by applying the changes
      const currentState = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, index])
      }

      // Apply the changes
      for (const [[_, [value, index]], multiplicity] of changes) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      const currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      // Check that indices are still in lexicographic order after the changes
      expect(checkLexicographicOrder(currentStateArray)).toBe(true)
    })

    it('should handle limit and offset correctly', () => {
      const graph = new D2()
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithFractionalIndex((a, b) => a.value.localeCompare(b.value), {
          limit: 3,
          offset: 1,
        }),
        output((message) => {
          allMessages.push(message)
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      graph.run()

      // Initial result should be b, c, d (offset 1, limit 3)
      const initialResult = allMessages[0].getInner()
      expect(initialResult.length).toBe(3)

      // Check that indices are in lexicographic order
      expect(checkLexicographicOrder(initialResult)).toBe(true)

      // Check that we have the correct elements (b, c, d)
      const initialIds = new Set(
        initialResult.map(([[_, [value, __]]]) => value.id),
      )
      expect(initialIds.has(1)).toBe(false) // 'a' should be excluded (offset)
      expect(initialIds.has(2)).toBe(true) // 'b' should be included
      expect(initialIds.has(3)).toBe(true) // 'c' should be included
      expect(initialIds.has(4)).toBe(true) // 'd' should be included
      expect(initialIds.has(5)).toBe(false) // 'e' should be excluded (limit)

      // Now let's add a new element that should be included in the result
      input.sendData(
        new MultiSet([
          [[null, { id: 6, value: 'c+' }], 1], // This should be between c and d
        ]),
      )
      graph.run()

      // Check the changes
      const changes = allMessages[1].getInner()

      // We should only emit as many changes as we received
      // We received 1 change (1 addition)
      // Since we have a limit, this will push out 1 element, so we'll emit 2 changes
      // This is still optimal as we're only emitting the minimum necessary changes
      expect(changes.length).toBe(2) // 1 removal + 1 addition

      // Find the removal and addition
      const removal = changes.find(([_, multiplicity]) => multiplicity < 0)
      const addition = changes.find(([_, multiplicity]) => multiplicity > 0)

      // Check that we removed 'd' and added 'c+'
      expect(removal?.[0][1][0].value).toBe('d')
      expect(addition?.[0][1][0].value).toBe('c+')

      // Check that the ids are correct
      expect(removal?.[0][1][0].id).toBe(4) // 'd' has id 4
      expect(addition?.[0][1][0].id).toBe(6) // 'c+' has id 6

      // The new element reuses the index of the removed element
      expect(addition?.[0][1][1]).toBe(removal?.[0][1][1])

      // Reconstruct the current state by applying the changes
      const currentState = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, index])
      }

      // Apply the changes
      for (const [[_, [value, index]], multiplicity] of changes) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      const currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      // Check that indices are still in lexicographic order after the changes
      expect(checkLexicographicOrder(currentStateArray)).toBe(true)
    })

    it('should handle elements moving positions correctly', () => {
      const graph = new D2()
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithFractionalIndex((a, b) => a.value.localeCompare(b.value)),
        output((message) => {
          allMessages.push(message)
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      graph.run()

      // Initial result should have all elements with fractional indices
      const initialResult = allMessages[0].getInner()
      expect(initialResult.length).toBe(5)

      // Check that indices are in lexicographic order
      expect(checkLexicographicOrder(initialResult)).toBe(true)

      // Store the initial indices for later comparison
      const initialIndices = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        initialIndices.set(value.id, index)
      }

      // Now let's swap 'b' and 'd'
      input.sendData(
        new MultiSet([
          [[null, { id: 2, value: 'd+' }], 1], // 'b' becomes 'd+'
          [[null, { id: 2, value: 'b' }], -1], // Remove old 'b'
          [[null, { id: 4, value: 'b+' }], 1], // 'd' becomes 'b+'
          [[null, { id: 4, value: 'd' }], -1], // Remove old 'd'
        ]),
      )
      graph.run()

      // Check the changes
      const changes = allMessages[1].getInner()

      // We should only emit as many changes as we received
      // We received 4 changes (2 additions, 2 removals)
      // We should emit at most 4 changes
      expect(changes.length).toBeLessThanOrEqual(4)
      expect(changes.length).toBe(4) // 2 removals + 2 additions

      // Find the removals and additions
      const removals = changes.filter(([_, multiplicity]) => multiplicity < 0)
      const additions = changes.filter(([_, multiplicity]) => multiplicity > 0)
      expect(removals.length).toBe(2)
      expect(additions.length).toBe(2)

      // Check that we removed 'b' and 'd'
      const removedValues = new Set(
        removals.map(([[_, [value, __]]]) => value.value),
      )
      expect(removedValues.has('b')).toBe(true)
      expect(removedValues.has('d')).toBe(true)

      // Check that we added 'b+' and 'd+'
      const addedValues = new Set(
        additions.map(([[_, [value, __]]]) => value.value),
      )
      expect(addedValues.has('b+')).toBe(true)
      expect(addedValues.has('d+')).toBe(true)

      // Find the specific removals and additions
      const bRemoval = removals.find(
        ([[_, [value, __]]]) => value.value === 'b',
      )
      const dRemoval = removals.find(
        ([[_, [value, __]]]) => value.value === 'd',
      )
      const bPlusAddition = additions.find(
        ([[_, [value, __]]]) => value.value === 'b+',
      )
      const dPlusAddition = additions.find(
        ([[_, [value, __]]]) => value.value === 'd+',
      )

      // The elements reuse their indices
      expect(bPlusAddition?.[0][1][1]).toBe(bRemoval?.[0][1][1])
      expect(dPlusAddition?.[0][1][1]).toBe(dRemoval?.[0][1][1])

      // Check that we only emitted changes for the elements that moved
      const changedIds = new Set()
      for (const [[_, [value, __]], multiplicity] of changes) {
        changedIds.add(value.id)
      }
      expect(changedIds.size).toBe(2)
      expect(changedIds.has(2)).toBe(true)
      expect(changedIds.has(4)).toBe(true)

      // Reconstruct the current state by applying the changes
      const currentState = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, index])
      }

      // Apply the changes
      for (const [[_, [value, index]], multiplicity] of changes) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      const currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      // Check that indices are still in lexicographic order after the changes
      expect(checkLexicographicOrder(currentStateArray)).toBe(true)
    })

    it('should maintain lexicographic order through multiple updates', () => {
      const graph = new D2()
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithFractionalIndex((a, b) => a.value.localeCompare(b.value)),
        output((message) => {
          allMessages.push(message)
        }),
      )

      graph.finalize()

      // Initial data - a, c, e, g, i
      input.sendData(
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
          [[null, { id: 7, value: 'g' }], 1],
          [[null, { id: 9, value: 'i' }], 1],
        ]),
      )
      graph.run()

      // Initial result should have all elements with fractional indices
      const initialResult = allMessages[0].getInner()
      expect(initialResult.length).toBe(5)

      // Check that indices are in lexicographic order
      expect(checkLexicographicOrder(initialResult)).toBe(true)

      // Keep track of the current state
      let currentState = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, index])
      }

      // Update 1: Insert elements between existing ones - b, d, f, h
      input.sendData(
        new MultiSet([
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 6, value: 'f' }], 1],
          [[null, { id: 8, value: 'h' }], 1],
        ]),
      )
      graph.run()

      // Check the changes
      const changes1 = allMessages[1].getInner()

      // We should only emit as many changes as we received
      // We received 4 changes (4 additions)
      // We should emit at most 4 changes
      expect(changes1.length).toBeLessThanOrEqual(4)
      expect(changes1.length).toBe(4) // 4 additions

      // Apply the changes to our current state
      for (const [[_, [value, index]], multiplicity] of changes1) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      let currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      // Check that indices are still in lexicographic order after the changes
      expect(checkLexicographicOrder(currentStateArray)).toBe(true)

      // Update 2: Move some elements around
      input.sendData(
        new MultiSet([
          [[null, { id: 3, value: 'j' }], 1], // Move 'c' to after 'i'
          [[null, { id: 3, value: 'c' }], -1], // Remove old 'c'
          [[null, { id: 7, value: 'a-' }], 1], // Move 'g' to before 'a'
          [[null, { id: 7, value: 'g' }], -1], // Remove old 'g'
        ]),
      )
      graph.run()

      // Check the changes
      const changes2 = allMessages[2].getInner()

      // We should only emit as many changes as we received
      // We received 4 changes (2 additions, 2 removals)
      // We should emit at most 4 changes
      expect(changes2.length).toBeLessThanOrEqual(4)
      expect(changes2.length).toBe(4) // 2 removals + 2 additions

      // Apply the changes to our current state
      for (const [[_, [value, index]], multiplicity] of changes2) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      // Check that indices are still in lexicographic order after the changes
      expect(checkLexicographicOrder(currentStateArray)).toBe(true)

      // Update 3: Remove some elements and add new ones
      input.sendData(
        new MultiSet([
          [[null, { id: 2, value: 'b' }], -1], // Remove 'b'
          [[null, { id: 4, value: 'd' }], -1], // Remove 'd'
          [[null, { id: 10, value: 'k' }], 1], // Add 'k' at the end
          [[null, { id: 11, value: 'c-' }], 1], // Add 'c-' between 'b' and 'd'
        ]),
      )
      graph.run()

      // Check the changes
      const changes3 = allMessages[3].getInner()

      // We should only emit as many changes as we received
      // We received 4 changes (2 additions, 2 removals)
      // We should emit at most 4 changes
      expect(changes3.length).toBeLessThanOrEqual(4)
      expect(changes3.length).toBe(4) // 2 removals + 2 additions

      // Apply the changes to our current state
      for (const [[_, [value, index]], multiplicity] of changes3) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      // Check that indices are still in lexicographic order after all changes
      expect(checkLexicographicOrder(currentStateArray)).toBe(true)
    })

    it('should maintain correct order when cycling through multiple changes', () => {
      const graph = new D2()
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithFractionalIndex((a, b) => a.value.localeCompare(b.value)),
        output((message) => {
          allMessages.push(message)
        }),
      )

      graph.finalize()

      // Create initial data with 12 items in alphabetical order
      const initialItems: [[null, { id: number; value: string }], number][] = []
      for (let i = 0; i < 12; i++) {
        const letter = String.fromCharCode(97 + i) // 'a' through 'l'
        initialItems.push([[null, { id: i + 1, value: letter }], 1])
      }

      // Send initial data
      input.sendData(new MultiSet(initialItems))
      graph.run()

      // Initial result should have all 12 elements with fractional indices
      const initialResult = allMessages[0].getInner()
      expect(initialResult.length).toBe(12)

      // Check that indices are in lexicographic order
      expect(checkLexicographicOrder(initialResult)).toBe(true)

      // Verify the initial order is a-l
      verifyOrder(initialResult, [
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
        'h',
        'i',
        'j',
        'k',
        'l',
      ])

      // Keep track of the current state
      let currentState = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, index])
      }

      // Now cycle through 10 changes, moving one item down one position each time
      // We'll move item 'a' down through the list
      let currentItem = { id: 1, value: 'a' }
      let expectedOrder = [
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
        'h',
        'i',
        'j',
        'k',
        'l',
      ]

      for (let i = 0; i < 10; i++) {
        // Calculate the new position for the item
        const currentPos = expectedOrder.indexOf(currentItem.value)
        const newPos = Math.min(currentPos + 1, expectedOrder.length - 1)

        // Create a new value that will sort to the new position
        // We'll use the next letter plus the current letter to ensure correct sorting
        const nextLetter = expectedOrder[newPos]
        const newValue = nextLetter + currentItem.value

        // Update the expected order
        expectedOrder.splice(currentPos, 1) // Remove from current position
        expectedOrder.splice(newPos, 0, newValue) // Insert at new position

        // Send the change
        input.sendData(
          new MultiSet([
            [[null, { id: currentItem.id, value: newValue }], 1], // Add with new value
            [[null, { id: currentItem.id, value: currentItem.value }], -1], // Remove old value
          ]),
        )
        graph.run()

        // Check the changes
        const changes = allMessages[i + 1].getInner()

        // We should only emit as many changes as we received (2)
        expect(changes.length).toBeLessThanOrEqual(2)
        expect(changes.length).toBe(2) // 1 removal + 1 addition

        // Apply the changes to our current state
        for (const [[_, [value, index]], multiplicity] of changes) {
          if (multiplicity < 0) {
            // Remove
            currentState.delete(JSON.stringify(value))
          } else {
            // Add
            currentState.set(JSON.stringify(value), [value, index])
          }
        }

        // Convert to array for checks
        const currentStateArray = Array.from(currentState.values()).map(
          ([value, index]) => [[null, [value, index]], 1],
        )

        // Check that indices are still in lexicographic order after the change
        expect(checkLexicographicOrder(currentStateArray)).toBe(true)

        // Verify the order matches our expected order
        verifyOrder(currentStateArray, expectedOrder)

        // Update the current item for the next iteration
        currentItem = { id: currentItem.id, value: newValue }
      }
    })

    it('should handle insertion at the start of the sorted collection', () => {
      const graph = new D2()
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithFractionalIndex((a, b) => a.value.localeCompare(b.value)),
        output((message) => {
          allMessages.push(message)
        }),
      )

      graph.finalize()

      // Initial data - b, c, d, e
      input.sendData(
        new MultiSet([
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      graph.run()

      // Initial result should have all elements with fractional indices
      const initialResult = allMessages[0].getInner()
      expect(initialResult.length).toBe(4)

      // Check that indices are in lexicographic order
      expect(checkLexicographicOrder(initialResult)).toBe(true)

      // Keep track of the current state
      let currentState = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, index])
      }

      // Update: Insert element at the start - 'a'
      input.sendData(
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1], // This should be inserted at the start
        ]),
      )
      graph.run()

      // Check the changes
      const changes = allMessages[1].getInner()

      // We should only emit as many changes as we received (1 addition)
      expect(changes.length).toBe(1)

      // Apply the changes to our current state
      for (const [[_, [value, index]], multiplicity] of changes) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      let currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      expect(checkLexicographicOrder(currentStateArray)).toBe(true)

      // Verify the order of elements
      const expectedOrder = ['a', 'b', 'c', 'd', 'e']
      verifyOrder(currentStateArray, expectedOrder)

      // Check that the new element 'a' has an index that is lexicographically before 'b'
      const aValue = { id: 1, value: 'a' }
      const bValue = { id: 2, value: 'b' }
      const aIndex = currentState.get(JSON.stringify(aValue))[1]
      const bIndex = currentState.get(JSON.stringify(bValue))[1]

      // Directly check that 'a' comes before 'b' lexicographically
      expect(aIndex < bIndex).toBe(true)
    })

    it('should handle multiple insertion at the start of the sorted collection', () => {
      const graph = new D2()
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithFractionalIndex((a, b) => a.value.localeCompare(b.value)),
        output((message) => {
          allMessages.push(message)
        }),
      )

      graph.finalize()

      // Initial data - b, c, d, e
      input.sendData(
        new MultiSet([
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
          [[null, { id: 6, value: 'f' }], 1],
        ]),
      )
      graph.run()

      // Initial result should have all elements with fractional indices
      const initialResult = allMessages[0].getInner()
      expect(initialResult.length).toBe(4)

      // Check that indices are in lexicographic order
      expect(checkLexicographicOrder(initialResult)).toBe(true)

      // Keep track of the current state
      let currentState = new Map()
      for (const [[_, [value, index]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, index])
      }

      // Update: Insert element at the start - 'a'
      input.sendData(
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1], // This should be inserted at the start
          [[null, { id: 2, value: 'b' }], 1], // This should be inserted at the start
        ]),
      )
      graph.run()

      // Check the changes
      const changes = allMessages[1].getInner()

      // We should only emit as many changes as we received (1 addition)
      expect(changes.length).toBe(2)

      // Apply the changes to our current state
      for (const [[_, [value, index]], multiplicity] of changes) {
        if (multiplicity < 0) {
          // Remove
          currentState.delete(JSON.stringify(value))
        } else {
          // Add
          currentState.set(JSON.stringify(value), [value, index])
        }
      }

      // Convert to array for lexicographic order check
      let currentStateArray = Array.from(currentState.values()).map(
        ([value, index]) => [[null, [value, index]], 1],
      )

      expect(checkLexicographicOrder(currentStateArray)).toBe(true)

      // Verify the order of elements
      const expectedOrder = ['a', 'b', 'c', 'd', 'e', 'f']
      verifyOrder(currentStateArray, expectedOrder)
    })
  })
})
