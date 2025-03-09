import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { Query } from '../../src/d2ql/index.js'
import { compileQuery } from '../../src/d2ql/compiler.js'
import { MultiSet } from '../../src/multiset.js'
import { output } from '../../src/operators/index.js'
import { MessageType } from '../../src/types.js'

describe('D2QL', () => {
  describe('orderBy functionality', () => {
    test('error when using limit without orderBy', () => {
      const query: Query = {
        select: ['@id', '@name', '@age'],
        from: 'users',
        limit: 1, // No orderBy clause
      }

      // Compiling the query should throw an error
      expect(() => {
        const graph = new D2({ initialFrontier: 0 })
        const input = graph.newInput<{
          id: number
          name: string
          age: number
        }>()
        compileQuery(query, { users: input })
      }).toThrow(
        'LIMIT and OFFSET require an ORDER BY clause to ensure deterministic results',
      )
    })

    test('error when using offset without orderBy', () => {
      const query: Query = {
        select: ['@id', '@name', '@age'],
        from: 'users',
        offset: 1, // No orderBy clause
      }

      // Compiling the query should throw an error
      expect(() => {
        const graph = new D2({ initialFrontier: 0 })
        const input = graph.newInput<{
          id: number
          name: string
          age: number
        }>()
        compileQuery(query, { users: input })
      }).toThrow(
        'LIMIT and OFFSET require an ORDER BY clause to ensure deterministic results',
      )
    })

    test('initial results with orderBy', () => {
      const query: Query = {
        select: ['@id', '@value'],
        from: 'input',
        orderBy: '@value',
      }

      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<{
        id: number
        value: string
      }>()
      let latestMessage: any = null

      const pipeline = compileQuery(query, { input })
      pipeline.pipe(
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
          [{ id: 1, value: 'a' }, 1],
          [{ id: 2, value: 'z' }, 1],
          [{ id: 3, value: 'b' }, 1],
          [{ id: 4, value: 'y' }, 1],
          [{ id: 5, value: 'c' }, 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(
        sortResults(result, (a, b) => a.value.localeCompare(b.value)),
      ).toEqual([
        [{ id: 1, value: 'a' }, 1],
        [{ id: 3, value: 'b' }, 1],
        [{ id: 5, value: 'c' }, 1],
        [{ id: 4, value: 'y' }, 1],
        [{ id: 2, value: 'z' }, 1],
      ])
    })

    test('initial results with a keyBy and orderBy', () => {
      const query: Query = {
        select: ['@id', '@value'],
        from: 'input',
        keyBy: '@id',
        orderBy: '@value',
      }

      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<{
        id: number
        value: string
      }>()
      let latestMessage: any = null

      const pipeline = compileQuery(query, { input })
      pipeline.pipe(
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
          [{ id: 1, value: 'a' }, 1],
          [{ id: 2, value: 'z' }, 1],
          [{ id: 3, value: 'b' }, 1],
          [{ id: 4, value: 'y' }, 1],
          [{ id: 5, value: 'c' }, 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(
        sortResults(result, (a, b) => a[1].value.localeCompare(b[1].value)),
      ).toEqual([
        [[1, { id: 1, value: 'a' }], 1],
        [[3, { id: 3, value: 'b' }], 1],
        [[5, { id: 5, value: 'c' }], 1],
        [[4, { id: 4, value: 'y' }], 1],
        [[2, { id: 2, value: 'z' }], 1],
      ])
    })

    test('initial results with orderBy and numeric index', () => {
      const query: Query = {
        select: ['@id', '@value', { index: { ORDER_INDEX: 'numeric' } }],
        from: 'input',
        orderBy: '@value',
      }

      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<{
        id: number
        value: string
      }>()
      let latestMessage: any = null

      const pipeline = compileQuery(query, { input })
      pipeline.pipe(
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
          [{ id: 1, value: 'a' }, 1],
          [{ id: 2, value: 'z' }, 1],
          [{ id: 3, value: 'b' }, 1],
          [{ id: 4, value: 'y' }, 1],
          [{ id: 5, value: 'c' }, 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(
        sortResults(result, (a, b) => a.value.localeCompare(b.value)),
      ).toEqual([
        [{ id: 1, value: 'a', index: 0 }, 1],
        [{ id: 3, value: 'b', index: 1 }, 1],
        [{ id: 5, value: 'c', index: 2 }, 1],
        [{ id: 4, value: 'y', index: 3 }, 1],
        [{ id: 2, value: 'z', index: 4 }, 1],
      ])
    })

    test('initial results with a keyBy and orderBy and numeric index', () => {
      const query: Query = {
        select: ['@id', '@value', { index: { ORDER_INDEX: 'numeric' } }],
        from: 'input',
        keyBy: '@id',
        orderBy: '@value',
      }

      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<{
        id: number
        value: string
      }>()
      let latestMessage: any = null

      const pipeline = compileQuery(query, { input })
      pipeline.pipe(
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
          [{ id: 1, value: 'a' }, 1],
          [{ id: 2, value: 'z' }, 1],
          [{ id: 3, value: 'b' }, 1],
          [{ id: 4, value: 'y' }, 1],
          [{ id: 5, value: 'c' }, 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(
        sortResults(result, (a, b) => a[1].value.localeCompare(b[1].value)),
      ).toEqual([
        [[1, { id: 1, value: 'a', index: 0 }], 1],
        [[3, { id: 3, value: 'b', index: 1 }], 1],
        [[5, { id: 5, value: 'c', index: 2 }], 1],
        [[4, { id: 4, value: 'y', index: 3 }], 1],
        [[2, { id: 2, value: 'z', index: 4 }], 1],
      ])
    })

    test('initial results with orderBy and fractional index', () => {
      const query: Query = {
        select: ['@id', '@value', { index: { ORDER_INDEX: 'fractional' } }],
        from: 'input',
        orderBy: '@value',
      }

      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<{
        id: number
        value: string
      }>()
      let latestMessage: any = null

      const pipeline = compileQuery(query, { input })
      pipeline.pipe(
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
          [{ id: 1, value: 'a' }, 1],
          [{ id: 2, value: 'z' }, 1],
          [{ id: 3, value: 'b' }, 1],
          [{ id: 4, value: 'y' }, 1],
          [{ id: 5, value: 'c' }, 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(
        sortResults(result, (a, b) => a.value.localeCompare(b.value)),
      ).toEqual([
        [{ id: 1, value: 'a', index: 'a0' }, 1],
        [{ id: 3, value: 'b', index: 'a1' }, 1],
        [{ id: 5, value: 'c', index: 'a2' }, 1],
        [{ id: 4, value: 'y', index: 'a3' }, 1],
        [{ id: 2, value: 'z', index: 'a4' }, 1],
      ])
    })

    test('initial results with a keyBy and orderBy and fractional index', () => {
      const query: Query = {
        select: ['@id', '@value', { index: { ORDER_INDEX: 'fractional' } }],
        from: 'input',
        keyBy: '@id',
        orderBy: '@value',
      }

      const graph = new D2({ initialFrontier: 0 })
      const input = graph.newInput<{
        id: number
        value: string
      }>()
      let latestMessage: any = null

      const pipeline = compileQuery(query, { input })
      pipeline.pipe(
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
          [{ id: 1, value: 'a' }, 1],
          [{ id: 2, value: 'z' }, 1],
          [{ id: 3, value: 'b' }, 1],
          [{ id: 4, value: 'y' }, 1],
          [{ id: 5, value: 'c' }, 1],
        ]),
      )
      input.sendFrontier(1)

      graph.run()

      expect(latestMessage).not.toBeNull()

      const result = latestMessage.collection.getInner()

      expect(
        sortResults(result, (a, b) => a[1].value.localeCompare(b[1].value)),
      ).toEqual([
        [[1, { id: 1, value: 'a', index: 'a0' }], 1],
        [[3, { id: 3, value: 'b', index: 'a1' }], 1],
        [[5, { id: 5, value: 'c', index: 'a2' }], 1],
        [[4, { id: 4, value: 'y', index: 'a3' }], 1],
        [[2, { id: 2, value: 'z', index: 'a4' }], 1],
      ])
    })
  })
})

/**
 * Sort results by multiplicity and then key
 */
function sortResults(
  results: [value: any, multiplicity: number][],
  comparator: (a: any, b: any) => number,
) {
  return [...results]
    .sort(
      ([_aValue, aMultiplicity], [_bValue, bMultiplicity]) =>
        aMultiplicity - bMultiplicity,
    )
    .sort(([aValue, _aMultiplicity], [bValue, _bMultiplicity]) =>
      comparator(aValue, bValue),
    )
}
