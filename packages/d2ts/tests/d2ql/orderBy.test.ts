import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { Query } from '../../src/d2ql/index.js'
import { compileQuery } from '../../src/d2ql/compiler.js'

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
  })
})
