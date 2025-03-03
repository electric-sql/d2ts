import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Message, MessageType } from '../../src/types.js'
import { output } from '../../src/operators/index.js'
import { v, Antichain } from '../../src/order.js'
import { Query } from '../../src/d2ql/index.js'
import { compileQuery } from '../../src/d2ql/compiler.js'

// Sample user type for tests
type User = {
  id: number
  name: string
  age: number
  email: string
  active: boolean
  joined_date: string
  preferences: string // JSON string for testing JSON_EXTRACT
}

// Sample data for tests
const sampleUsers: User[] = [
  {
    id: 1,
    name: 'Alice',
    age: 25,
    email: 'alice@example.com',
    active: true,
    joined_date: '2023-01-15',
    preferences: '{"theme":"dark","notifications":true,"language":"en"}',
  },
  {
    id: 2,
    name: 'Bob',
    age: 19,
    email: 'bob@example.com',
    active: true,
    joined_date: '2023-02-20',
    preferences: '{"theme":"light","notifications":false,"language":"fr"}',
  },
  {
    id: 3,
    name: 'Charlie',
    age: 30,
    email: 'charlie@example.com',
    active: false,
    joined_date: '2022-11-05',
    preferences: '{"theme":"system","notifications":true,"language":"es"}',
  },
  {
    id: 4,
    name: 'Dave',
    age: 22,
    email: 'dave@example.com',
    active: true,
    joined_date: '2023-03-10',
    preferences: '{"theme":"dark","notifications":true,"language":"de"}',
  },
]

describe('D2QL Function Integration', () => {
  /**
   * Helper function to run a query and return results
   */
  function runQuery(query: Query): any[] {
    const graph = new D2({ initialFrontier: v([0, 0]) })
    const input = graph.newInput<User>()
    const pipeline = compileQuery(query, { [query.from]: input })

    const messages: Message<any>[] = []
    pipeline.pipe(
      output((message) => {
        messages.push(message)
      }),
    )

    graph.finalize()

    input.sendData(
      v([1, 0]),
      new MultiSet(sampleUsers.map((user) => [user, 1])),
    )
    input.sendFrontier(new Antichain([v([1, 0])]))

    graph.run()

    // Return only the data (not the counts)
    const dataMessages = messages.filter((m) => m.type === MessageType.DATA)
    if (dataMessages.length === 0) return []

    return dataMessages[0].data.collection.getInner().map(([data]) => data)
  }

  describe('String functions', () => {
    test('UPPER function', () => {
      const query: Query = {
        select: ['@id', { upper_name: { UPPER: '@name' } }],
        from: 'users',
      }

      const results = runQuery(query)

      expect(results).toHaveLength(4)
      expect(results).toContainEqual({
        id: 1,
        upper_name: 'ALICE',
      })
      expect(results).toContainEqual({
        id: 2,
        upper_name: 'BOB',
      })
    })

    test('LOWER function', () => {
      const query: Query = {
        select: ['@id', { lower_email: { LOWER: '@email' } }],
        from: 'users',
      }

      const results = runQuery(query)

      expect(results).toHaveLength(4)
      expect(results).toContainEqual({
        id: 1,
        lower_email: 'alice@example.com',
      })
    })

    test('LENGTH function on string', () => {
      const query: Query = {
        select: ['@id', '@name', { name_length: { LENGTH: '@name' } }],
        from: 'users',
      }

      const results = runQuery(query)

      expect(results).toHaveLength(4)
      expect(results).toContainEqual({
        id: 1,
        name: 'Alice',
        name_length: 5,
      })
      expect(results).toContainEqual({
        id: 3,
        name: 'Charlie',
        name_length: 7,
      })
    })

    test('CONCAT function', () => {
      const query: Query = {
        select: [
          '@id',
          { full_details: { CONCAT: ['@name', ' (', '@email', ')'] } },
        ],
        from: 'users',
      }

      const results = runQuery(query)

      expect(results).toHaveLength(4)
      expect(results).toContainEqual({
        id: 1,
        full_details: 'Alice (alice@example.com)',
      })
    })
  })

  describe('Value processing functions', () => {
    test('COALESCE function', () => {
      // For this test, create a query that would produce some null values
      const query: Query = {
        select: [
          '@id',
          {
            status: {
              COALESCE: [
                {
                  CONCAT: [
                    {
                      UPPER: '@name',
                    },
                    ' IS INACTIVE',
                  ],
                },
                'UNKNOWN',
              ],
            },
          },
        ],
        from: 'users',
        where: ['@active', '=', false],
      }

      const results = runQuery(query)

      expect(results).toHaveLength(1) // Only Charlie is inactive
      expect(results[0].status).toBe('CHARLIE IS INACTIVE')
    })

    test('DATE function', () => {
      const query: Query = {
        select: ['@id', '@name', { joined: { DATE: '@joined_date' } }],
        from: 'users',
        orderBy: { id: 'asc' },
      }

      const results = runQuery(query)

      expect(results).toHaveLength(4)

      // Verify that each result has a joined field with a Date object
      results.forEach((result) => {
        expect(result.joined).toBeInstanceOf(Date)
      })

      // Check specific dates
      expect(results[0].id).toBe(1) // Alice
      expect(results[0].joined.getFullYear()).toBe(2023)
      expect(results[0].joined.getMonth()).toBe(0) // January (0-indexed)
      expect(results[0].joined.getDate()).toBe(15)
    })
  })

  describe('JSON functions', () => {
    test('JSON_EXTRACT function', () => {
      const query: Query = {
        select: [
          '@id',
          '@name',
          { theme: { JSON_EXTRACT: ['@preferences', 'theme'] } },
        ],
        from: 'users',
      }

      const results = runQuery(query)

      expect(results).toHaveLength(4)
      expect(results).toContainEqual({
        id: 1,
        name: 'Alice',
        theme: 'dark',
      })
      expect(results).toContainEqual({
        id: 2,
        name: 'Bob',
        theme: 'light',
      })
    })

    test('JSON_EXTRACT_PATH function (alias)', () => {
      const query: Query = {
        select: [
          '@id',
          {
            notifications_enabled: {
              JSON_EXTRACT_PATH: ['@preferences', 'notifications'],
            },
          },
        ],
        from: 'users',
        where: ['@active', '=', true],
      }

      const results = runQuery(query)

      expect(results).toHaveLength(3) // Alice, Bob, Dave
      // Bob has notifications disabled
      expect(results).toContainEqual({
        id: 2,
        notifications_enabled: false,
      })
      // Alice and Dave have notifications enabled
      expect(
        results.filter((r) => r.notifications_enabled === true).length,
      ).toBe(2)
    })
  })

  describe('Using functions in WHERE clauses', () => {
    test('Filter with UPPER function', () => {
      const query: Query = {
        select: ['@id', '@name'],
        from: 'users',
        where: [{ UPPER: '@name' }, '=', 'BOB'],
      }

      const results = runQuery(query)

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe(2)
      expect(results[0].name).toBe('Bob')
    })

    test('Filter with LENGTH function', () => {
      const query: Query = {
        select: ['@id', '@name'],
        from: 'users',
        where: [{ LENGTH: '@name' }, '>', 5],
      }

      const results = runQuery(query)

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe(3)
      expect(results[0].name).toBe('Charlie')
    })

    test('Filter with JSON_EXTRACT function', () => {
      const query: Query = {
        select: ['@id', '@name'],
        from: 'users',
        where: [{ JSON_EXTRACT: ['@preferences', 'theme'] }, '=', 'dark'],
      }

      const results = runQuery(query)

      expect(results).toHaveLength(2) // Alice and Dave
      expect(results.map((r) => r.id).sort()).toEqual([1, 4])
    })

    test('Complex filter with multiple functions', () => {
      const query: Query = {
        select: ['@id', '@name', '@email'],
        from: 'users',
        where: [
          { LENGTH: '@name' },
          '<',
          6,
          'and',
          { JSON_EXTRACT_PATH: ['@preferences', 'notifications'] },
          '=',
          true,
        ],
      }

      const results = runQuery(query)

      // It turns out both Alice and Dave match our criteria
      expect(results).toHaveLength(2)
      // Sort results by ID for consistent testing
      const sortedResults = [...results].sort((a, b) => a.id - b.id)

      // Check that Alice is included
      expect(sortedResults[0].id).toBe(1)
      expect(sortedResults[0].name).toBe('Alice')

      // Check that Dave is included
      expect(sortedResults[1].id).toBe(4)
      expect(sortedResults[1].name).toBe('Dave')

      // Verify that both users have name length < 6 and notifications enabled
      results.forEach((result) => {
        expect(result.name.length).toBeLessThan(6)
        // We could also verify the JSON data directly if needed
      })
    })
  })
})
