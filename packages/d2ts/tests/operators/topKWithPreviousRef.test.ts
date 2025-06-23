import { describe, it, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { MessageType } from '../../src/types.js'
import { topKWithPreviousRef } from '../../src/operators/topKWithPreviousRef.js'
import { output } from '../../src/operators/index.js'

// Helper function to verify the linked list structure
function verifyLinkedListOrder(results: any[], expectedOrder: string[], nullValue: any = null) {
  // Extract values and their previous references
  const valuesWithPrevRefs = results.map(([[_, [value, prevRef]]]) => ({
    value: value.value,
    id: value.id,
    prevRef,
  }))

  // Build a map from reference to value for quick lookup
  const refToValue = new Map()
  for (const item of valuesWithPrevRefs) {
    refToValue.set(item.id, item.value)
  }

  // Follow the linked list to verify order
  // First, find the element with the specified null value as previous reference
  const firstElement = valuesWithPrevRefs.find(item => item.prevRef === nullValue)
  expect(firstElement).toBeDefined()
  expect(firstElement!.value).toBe(expectedOrder[0])

  // Then follow the chain
  let current = firstElement
  let actualOrder = [current!.value]
  
  for (let i = 1; i < expectedOrder.length; i++) {
    // Find the element that has current element as previous reference
    const next = valuesWithPrevRefs.find(item => item.prevRef === current!.id)
    expect(next).toBeDefined()
    actualOrder.push(next!.value)
    current = next
  }

  expect(actualOrder).toEqual(expectedOrder)
}

describe('Operators', () => {
  describe('TopKWithPreviousRef operation', () => {
    it('should create a linked list structure with previous references', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: null,
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data - c, a, b, d
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result should have all elements with linked list structure
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(4)

      // Verify the linked list structure follows alphabetical order: a -> b -> c -> d
      verifyLinkedListOrder(initialResult, ['a', 'b', 'c', 'd'], null)

      // Check specific previous references
      const valueToEntry = new Map()
      for (const [[_, [value, prevRef]]] of initialResult) {
        valueToEntry.set(value.value, { value, prevRef })
      }

      // 'a' should have null as previous reference (first element)
      expect(valueToEntry.get('a').prevRef).toBe(null)
      // 'b' should have 'a' as previous reference (id: 1)
      expect(valueToEntry.get('b').prevRef).toBe(1)
      // 'c' should have 'b' as previous reference (id: 2)
      expect(valueToEntry.get('c').prevRef).toBe(2)
      // 'd' should have 'c' as previous reference (id: 3)
      expect(valueToEntry.get('d').prevRef).toBe(3)
    })

    it('should handle element insertion correctly', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: null,
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data - a, c, e
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result: a -> c -> e
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(3)
      verifyLinkedListOrder(initialResult, ['a', 'c', 'e'], null)

      // Insert 'b' and 'd' in the middle
      input.sendData(
        1,
        new MultiSet([
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
        ]),
      )
      input.sendFrontier(2)
      graph.run()

      // Check the changes
      const changes = allMessages[1].collection.getInner()
      expect(changes.length).toBeGreaterThan(0)

      // Reconstruct the current state by applying the changes
      const currentState = new Map()
      for (const [[_, [value, prevRef]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, prevRef])
      }

      // Apply the changes
      for (const [[_, [value, prevRef]], multiplicity] of changes) {
        if (multiplicity < 0) {
          currentState.delete(JSON.stringify(value))
        } else {
          currentState.set(JSON.stringify(value), [value, prevRef])
        }
      }

      // Convert to array for verification
      const currentStateArray = Array.from(currentState.values()).map(
        ([value, prevRef]) => [[null, [value, prevRef]], 1],
      )

      // Verify the final linked list structure: a -> b -> c -> d -> e
      verifyLinkedListOrder(currentStateArray, ['a', 'b', 'c', 'd', 'e'], null)
    })

    it('should handle element removal correctly', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: null,
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result: a -> b -> c -> d -> e
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(5)
      verifyLinkedListOrder(initialResult, ['a', 'b', 'c', 'd', 'e'], null)

      // Remove 'b' and 'd'
      input.sendData(
        1,
        new MultiSet([
          [[null, { id: 2, value: 'b' }], -1],
          [[null, { id: 4, value: 'd' }], -1],
        ]),
      )
      input.sendFrontier(2)
      graph.run()

      // Check the changes
      const changes = allMessages[1].collection.getInner()
      expect(changes.length).toBeGreaterThan(0)

      // Reconstruct the current state
      const currentState = new Map()
      for (const [[_, [value, prevRef]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, prevRef])
      }

      // Apply the changes
      for (const [[_, [value, prevRef]], multiplicity] of changes) {
        if (multiplicity < 0) {
          currentState.delete(JSON.stringify(value))
        } else {
          currentState.set(JSON.stringify(value), [value, prevRef])
        }
      }

      // Convert to array for verification
      const currentStateArray = Array.from(currentState.values()).map(
        ([value, prevRef]) => [[null, [value, prevRef]], 1],
      )

      // Verify the final linked list structure: a -> c -> e
      verifyLinkedListOrder(currentStateArray, ['a', 'c', 'e'], null)
    })

    it('should handle element movement correctly', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: null,
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result: a -> b -> c -> d
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(4)
      verifyLinkedListOrder(initialResult, ['a', 'b', 'c', 'd'], null)

      // Move 'b' to the end by changing its value to 'z'
      input.sendData(
        1,
        new MultiSet([
          [[null, { id: 2, value: 'z' }], 1],
          [[null, { id: 2, value: 'b' }], -1],
        ]),
      )
      input.sendFrontier(2)
      graph.run()

      // Check the changes
      const changes = allMessages[1].collection.getInner()
      expect(changes.length).toBeGreaterThan(0)

      // Reconstruct the current state
      const currentState = new Map()
      for (const [[_, [value, prevRef]]] of initialResult) {
        currentState.set(JSON.stringify(value), [value, prevRef])
      }

      // Apply the changes
      for (const [[_, [value, prevRef]], multiplicity] of changes) {
        if (multiplicity < 0) {
          currentState.delete(JSON.stringify(value))
        } else {
          currentState.set(JSON.stringify(value), [value, prevRef])
        }
      }

      // Convert to array for verification
      const currentStateArray = Array.from(currentState.values()).map(
        ([value, prevRef]) => [[null, [value, prevRef]], 1],
      )

      // Verify the final linked list structure: a -> c -> d -> z
      verifyLinkedListOrder(currentStateArray, ['a', 'c', 'd', 'z'], null)
    })

    it('should handle limit correctly', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: null,
            limit: 3,
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Should only have first 3 elements: a -> b -> c
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(3)
      verifyLinkedListOrder(initialResult, ['a', 'b', 'c'], null)

      // Check that we have the right elements
      const values = initialResult.map(([[_, [value, __]]]) => value.value).sort()
      expect(values).toEqual(['a', 'b', 'c'])
    })

    it('should handle offset correctly', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: null,
            offset: 2,
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Should have elements starting from offset 2: c -> d -> e
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(3)
      verifyLinkedListOrder(initialResult, ['c', 'd', 'e'], null)

      // Check that we have the right elements
      const values = initialResult.map(([[_, [value, __]]]) => value.value).sort()
      expect(values).toEqual(['c', 'd', 'e'])

      // Verify that 'c' has null as previous reference since it's the first in the result
      const valueToEntry = new Map()
      for (const [[_, [value, prevRef]]] of initialResult) {
        valueToEntry.set(value.value, prevRef)
      }
      expect(valueToEntry.get('c')).toBe(null)
    })

    it('should handle limit and offset together', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: number; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: null,
            offset: 1,
            limit: 2,
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data - a, b, c, d, e
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 1, value: 'a' }], 1],
          [[null, { id: 2, value: 'b' }], 1],
          [[null, { id: 3, value: 'c' }], 1],
          [[null, { id: 4, value: 'd' }], 1],
          [[null, { id: 5, value: 'e' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Should have elements from offset 1 with limit 2: b -> c
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(2)
      verifyLinkedListOrder(initialResult, ['b', 'c'], null)

      // Check that we have the right elements
      const values = initialResult.map(([[_, [value, __]]]) => value.value).sort()
      expect(values).toEqual(['b', 'c'])

      // Verify that 'b' has null as previous reference since it's the first in the result
      const valueToEntry = new Map()
      for (const [[_, [value, prevRef]]] of initialResult) {
        valueToEntry.set(value.value, prevRef)
      }
      expect(valueToEntry.get('b')).toBe(null)
      expect(valueToEntry.get('c')).toBe(2) // Previous ref to 'b' (id: 2)
    })

    it('should handle different reference types', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<[null, { id: string; value: string }]>()
      const allMessages: any[] = []

      input.pipe(
        topKWithPreviousRef(
          (a, b) => a.value.localeCompare(b.value),
          {
            previousRef: (value) => value.id,
            nullValue: 'START',
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            allMessages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Initial data with string IDs
      input.sendData(
        0,
        new MultiSet([
          [[null, { id: 'item_3', value: 'c' }], 1],
          [[null, { id: 'item_1', value: 'a' }], 1],
          [[null, { id: 'item_2', value: 'b' }], 1],
        ]),
      )
      input.sendFrontier(1)
      graph.run()

      // Initial result: a -> b -> c
      const initialResult = allMessages[0].collection.getInner()
      expect(initialResult.length).toBe(3)
      verifyLinkedListOrder(initialResult, ['a', 'b', 'c'], 'START')

      // Check specific previous references with string IDs
      const valueToEntry = new Map()
      for (const [[_, [value, prevRef]]] of initialResult) {
        valueToEntry.set(value.value, { value, prevRef })
      }

      // 'a' should have 'START' as previous reference (first element)
      expect(valueToEntry.get('a').prevRef).toBe('START')
      // 'b' should have 'item_1' as previous reference
      expect(valueToEntry.get('b').prevRef).toBe('item_1')
      // 'c' should have 'item_2' as previous reference
      expect(valueToEntry.get('c').prevRef).toBe('item_2')
    })
  })
}) 