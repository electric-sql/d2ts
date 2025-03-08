import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { MessageType } from '../../src/types.js'
import { orderBy, output } from '../../src/operators/index.js'
import { KeyValue } from '../../src/types.js'

describe('Operators', () => {
  describe('OrderBy operation', () => {
    test('initial results with default comparator', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy<string, { id: number; value: string }, KeyValue<string, { id: number; value: string }>, string>(
          (item) => item.value
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()
      
      expect(sortResults(result)).toEqual([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key3', { id: 3, value: 'b' }], 1],
        [['key5', { id: 5, value: 'c' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key2', { id: 2, value: 'z' }], 1],
      ])
    })

    test('initial results with custom comparator', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy<string, { id: number; value: string }, KeyValue<string, { id: number; value: string }>, string>(
          (item) => item.value, 
          {
            comparator: (a, b) => b.localeCompare(a), // reverse order
          }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()
      
      expect(sortResults(result)).toEqual([
        [['key2', { id: 2, value: 'z' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key5', { id: 5, value: 'c' }], 1],
        [['key3', { id: 3, value: 'b' }], 1],
        [['key1', { id: 1, value: 'a' }], 1],
      ])
    })

    test('initial results with limit', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy<string, { id: number; value: string }, KeyValue<string, { id: number; value: string }>, string>(
          (item) => item.value, 
          { limit: 3 }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()
      
      expect(sortResults(result)).toEqual([
        [['key1', { id: 1, value: 'a' }], 1],
        [['key3', { id: 3, value: 'b' }], 1],
        [['key5', { id: 5, value: 'c' }], 1],
      ])
    })

    test('initial results with limit and offset', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy<string, { id: number; value: string }, KeyValue<string, { id: number; value: string }>, string>(
          (item) => item.value, 
          { limit: 2, offset: 2 }
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [['key1', { id: 1, value: 'a' }], 1],
          [['key2', { id: 2, value: 'z' }], 1],
          [['key3', { id: 3, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 5, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()
      
      expect(sortResults(result)).toEqual([
        [['key5', { id: 5, value: 'c' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
      ])
    })

    test('ordering by numeric property', () => {
      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<
        KeyValue<
          string,
          {
            id: number
            value: string
          }
        >
      >()
      let latestMessage: any = null

      input.pipe(
        orderBy<string, { id: number; value: string }, KeyValue<string, { id: number; value: string }>, number>(
          (item) => item.id
        ),
        output((message) => {
          if (message.type === MessageType.DATA) {
            latestMessage = message.data
          }
        }),
      )

      graph.finalize()

      input.sendData(
        0,
        new MultiSet([
          [['key1', { id: 5, value: 'a' }], 1],
          [['key2', { id: 3, value: 'z' }], 1],
          [['key3', { id: 1, value: 'b' }], 1],
          [['key4', { id: 4, value: 'y' }], 1],
          [['key5', { id: 2, value: 'c' }], 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()
      
      expect(sortResults(result)).toEqual([
        [['key3', { id: 1, value: 'b' }], 1],
        [['key5', { id: 2, value: 'c' }], 1],
        [['key2', { id: 3, value: 'z' }], 1],
        [['key4', { id: 4, value: 'y' }], 1],
        [['key1', { id: 5, value: 'a' }], 1],
      ])
    })
  })
})

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
