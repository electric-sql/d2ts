// @ts-nocheck
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { D2 } from '../../src/d2.js'
import { Version, Antichain, v } from '../../src/order.js'
import { MultiSet } from '../../src/multiset.js'
import { BetterSQLite3Wrapper } from '../../src/sqlite/database.js'
import { joinAll } from '../../src/sqlite/operators/join-all.js'
import { output } from '../../src/operators/index.js'
import { DataMessage, MessageType } from '../../src/types.js'
import Database from 'better-sqlite3'
import fs from 'fs'
import path from 'path'

describe('SQLite Operators', () => {
  describe('joinAll operation', () => {
    let db: Database.Database
    let dbWrapper: BetterSQLite3Wrapper

    beforeEach(() => {
      db = new Database(':memory:')
      dbWrapper = new BetterSQLite3Wrapper(db)
    })

    afterEach(() => {
      if (db) {
        db.close()
      }
    })

    it('should join multiple streams with inner join', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create three input streams
      const firstInput = graph.newInput<[string, number]>()
      const secondInput = graph.newInput<[string, string]>()
      const thirdInput = graph.newInput<[string, boolean]>()

      // Store messages for later verification
      const messages: DataMessage<
        [string, (number | string | boolean | null)[]]
      >[] = []

      // Join the streams using inner join
      firstInput.pipe(
        joinAll([secondInput, thirdInput], dbWrapper, 'inner'),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to first input stream
      const first = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
        [['c', 3], 1],
        [['d', 4], 1],
      ])
      firstInput.sendData(v([1, 0]), first)
      firstInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to second input stream
      const second = new MultiSet<[string, string]>([
        [['a', 'A'], 1],
        [['b', 'B'], 1],
        [['c', 'C'], 1],
        // 'd' is missing in second stream
      ])
      secondInput.sendData(v([1, 0]), second)
      secondInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to third input stream
      const third = new MultiSet<[string, boolean]>([
        [['a', true], 1],
        [['b', false], 1],
        // 'c' is missing in third stream
        [['d', true], 1],
      ])
      thirdInput.sendData(v([1, 0]), third)
      thirdInput.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph to compute the join
      await graph.run()

      // Verify results - with inner join, only keys present in all streams should be included
      // 'a' and 'b' are in all streams, 'c' is missing in the third stream, 'd' is missing in the second stream
      expect(messages.length).toBe(1)

      // Check first version ('t1')
      const innerJoinData6 = messages[0].collection.getInner()

      // Expect 2 distinct tuples (a & b)
      expect(innerJoinData6.length).toBe(2)

      // Verify the values
      const innerJoinSortedData6 = Array.from(innerJoinData6).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      // Check 'a'
      expect(innerJoinSortedData6[0][0][0]).toBe('a')
      expect(innerJoinSortedData6[0][0][1]).toEqual([1, 'A', true])

      // Check 'b'
      expect(innerJoinSortedData6[1][0][0]).toBe('b')
      expect(innerJoinSortedData6[1][0][1]).toEqual([2, 'B', false])
    })

    it('should join multiple streams with left join', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create three input streams
      const firstInput = graph.newInput<[string, number]>()
      const secondInput = graph.newInput<[string, string]>()
      const thirdInput = graph.newInput<[string, boolean]>()

      // Store messages for later verification
      const messages: DataMessage<
        [string, (number | string | boolean | null)[]]
      >[] = []

      // Join the streams using left join
      firstInput.pipe(
        joinAll([secondInput, thirdInput], dbWrapper, 'left'),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to first input stream
      const first = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
        [['c', 3], 1],
        [['d', 4], 1],
      ])
      firstInput.sendData(v([1, 0]), first)
      firstInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to second input stream
      const second = new MultiSet<[string, string]>([
        [['a', 'A'], 1],
        [['b', 'B'], 1],
        [['c', 'C'], 1],
        // 'd' is missing in second stream
      ])
      secondInput.sendData(v([1, 0]), second)
      secondInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to third input stream
      const third = new MultiSet<[string, boolean]>([
        [['a', true], 1],
        [['b', false], 1],
        // 'c' is missing in third stream
        [['d', true], 1],
      ])
      thirdInput.sendData(v([1, 0]), third)
      thirdInput.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph to compute the join
      await graph.run()

      // Verify results - with left join, all keys from first stream should be included
      expect(messages.length).toBe(1)

      // Check first version ('t1')
      const leftJoinData = messages[0].collection.getInner()

      // Expect 4 distinct tuples (a, b, c & d)
      expect(leftJoinData.length).toBe(4)

      // Verify the values
      const leftJoinSortedData = Array.from(leftJoinData).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      // Check 'a'
      expect(leftJoinSortedData[0][0][0]).toBe('a')
      expect(leftJoinSortedData[0][0][1]).toEqual([1, 'A', true])

      // Check 'b'
      expect(leftJoinSortedData[1][0][0]).toBe('b')
      expect(leftJoinSortedData[1][0][1]).toEqual([2, 'B', false])

      // Check 'c' - missing in third stream
      expect(leftJoinSortedData[2][0][0]).toBe('c')
      expect(leftJoinSortedData[2][0][1]).toEqual([3, 'C', null])

      // Check 'd' - missing in second stream
      expect(leftJoinSortedData[3][0][0]).toBe('d')
      expect(leftJoinSortedData[3][0][1]).toEqual([4, null, true])
    })

    it('should handle empty array of other streams', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create input stream
      const input = graph.newInput<[string, number]>()

      // Store messages for later verification
      const messages: DataMessage<[string, number[]]>[] = []

      // Join with empty array of other streams
      input.pipe(
        joinAll([], dbWrapper),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to input stream
      const data = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
        [['c', 3], 1],
      ])
      input.sendData(v([1, 0]), data)
      input.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph
      await graph.run()

      // Check results - should be original values wrapped in arrays
      expect(messages.length).toBe(1)

      const emptyJoinData = messages[0].collection.getInner()

      // Expect 3 distinct tuples (a, b & c)
      expect(emptyJoinData.length).toBe(3)

      // Verify the values
      const emptyJoinSortedData = Array.from(emptyJoinData).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      expect(emptyJoinSortedData[0][0][0]).toBe('a')
      expect(emptyJoinSortedData[0][0][1]).toEqual([1])
      expect(emptyJoinSortedData[0][1]).toBe(1)

      expect(emptyJoinSortedData[1][0][0]).toBe('b')
      expect(emptyJoinSortedData[1][0][1]).toEqual([2])
      expect(emptyJoinSortedData[1][1]).toBe(1)

      expect(emptyJoinSortedData[2][0][0]).toBe('c')
      expect(emptyJoinSortedData[2][0][1]).toEqual([3])
      expect(emptyJoinSortedData[2][1]).toBe(1)
    })

    it('should join streams across multiple versions', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create input streams
      const firstInput = graph.newInput<[string, number]>()
      const secondInput = graph.newInput<[string, string]>()

      // Store messages for later verification
      const messages: DataMessage<[string, (number | string | null)[]]>[] = []

      // Join the streams
      firstInput.pipe(
        joinAll([secondInput], dbWrapper),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to first input stream in version 1
      const first1 = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
      ])
      firstInput.sendData(v([1, 0]), first1)

      // Send data to second input stream in version 1
      const second1 = new MultiSet<[string, string]>([
        [['a', 'A'], 1],
        [['c', 'C'], 1],
      ])
      secondInput.sendData(v([1, 0]), second1)

      // Update frontiers to version 1
      firstInput.sendFrontier(new Antichain([v([1, 0])]))
      secondInput.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph for version 1
      await graph.run()

      // Send data to first input stream in version 2
      const first2 = new MultiSet<[string, number]>([
        [['a', 10], 1], // Updated value
        [['b', 2], -1], // Remove b
        [['d', 4], 1], // Add d
      ])
      firstInput.sendData(v([2, 0]), first2)

      // Send data to second input stream in version 2
      const second2 = new MultiSet<[string, string]>([
        [['b', 'B'], 1], // Add b (but too late, b already removed from first)
        [['c', 'C'], -1], // Remove c
        [['d', 'D'], 1], // Add d
      ])
      secondInput.sendData(v([2, 0]), second2)

      // Update frontiers to version 2
      firstInput.sendFrontier(new Antichain([v([2, 0])]))
      secondInput.sendFrontier(new Antichain([v([2, 0])]))

      // Run the graph for version 2
      await graph.run()

      // Check we have both versions
      expect(messages.length).toBe(2)

      // Verify version 1 results
      const v1Data = messages[0].collection.getInner()
      expect(v1Data.length).toBe(1) // Only 'a' is in both streams

      // Verify 'a' in version 1
      const t1Entries = Array.from(v1Data).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )
      expect(t1Entries[0][0][0]).toBe('a')
      expect(t1Entries[0][0][1]).toEqual([1, 'A'])

      // Verify version 2 results - we should have updated values
      const v2Data = messages[1].collection.getInner()
      // Should have 'd' in version 2, 'a' from version 1 will be updated, 'b' will be removed
      expect(v2Data.length).toBe(3) // 'a' and 'd' and possibly 'b'

      // Convert to array and sort for consistent comparison
      const t2Entries = Array.from(v2Data).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      // Check 'a' has updated value
      expect(t2Entries[0][0][0]).toBe('a')
      expect(t2Entries[0][0][1]).toContain(10)
      expect(t2Entries[0][0][1]).toContain('A')

      // Check 'b' is still present
      expect(t2Entries[1][0][0]).toBe('b')

      // Check 'd' was added
      expect(t2Entries[2][0][0]).toBe('d')
      expect(t2Entries[2][0][1]).toContain(4)
      expect(t2Entries[2][0][1]).toContain('D')
    })

    it('should join multiple streams with inner join', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create three input streams
      const firstInput = graph.newInput<[string, number]>()
      const secondInput = graph.newInput<[string, string]>()
      const thirdInput = graph.newInput<[string, boolean]>()

      // Store messages for later verification
      const messages: DataMessage<
        [string, (number | string | boolean | null)[]]
      >[] = []

      // Join the streams using inner join
      firstInput.pipe(
        joinAll([secondInput, thirdInput], dbWrapper, 'inner'),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to first input stream
      const first = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
        [['c', 3], 1],
        [['d', 4], 1],
      ])
      firstInput.sendData(v([1, 0]), first)
      firstInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to second input stream
      const second = new MultiSet<[string, string]>([
        [['a', 'A'], 1],
        [['b', 'B'], 1],
        [['c', 'C'], 1],
        // 'd' is missing in second stream
      ])
      secondInput.sendData(v([1, 0]), second)
      secondInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to third input stream
      const third = new MultiSet<[string, boolean]>([
        [['a', true], 1],
        [['b', false], 1],
        // 'c' is missing in third stream
        [['d', true], 1],
      ])
      thirdInput.sendData(v([1, 0]), third)
      thirdInput.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph to compute the join
      await graph.run()

      // Verify results - with inner join, only keys present in all streams should be included
      // 'a' and 'b' are in all streams, 'c' is missing in the third stream, 'd' is missing in the second stream
      expect(messages.length).toBe(1)

      // Check first version ('t1')
      const innerJoinData7 = messages[0].collection.getInner()

      // Expect 2 distinct tuples (a & b)
      expect(innerJoinData7.length).toBe(2)

      // Verify the values
      const innerJoinSortedData7 = Array.from(innerJoinData7).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      // Check 'a'
      expect(innerJoinSortedData7[0][0][0]).toBe('a')
      expect(innerJoinSortedData7[0][0][1]).toEqual([1, 'A', true])

      // Check 'b'
      expect(innerJoinSortedData7[1][0][0]).toBe('b')
      expect(innerJoinSortedData7[1][0][1]).toEqual([2, 'B', false])
    })

    it('should join multiple streams with left join', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create three input streams
      const firstInput = graph.newInput<[string, number]>()
      const secondInput = graph.newInput<[string, string]>()
      const thirdInput = graph.newInput<[string, boolean]>()

      // Store messages for later verification
      const messages: DataMessage<
        [string, (number | string | boolean | null)[]]
      >[] = []

      // Join the streams using left join
      firstInput.pipe(
        joinAll([secondInput, thirdInput], dbWrapper, 'left'),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to first input stream
      const first = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
        [['c', 3], 1],
        [['d', 4], 1],
      ])
      firstInput.sendData(v([1, 0]), first)
      firstInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to second input stream
      const second = new MultiSet<[string, string]>([
        [['a', 'A'], 1],
        [['b', 'B'], 1],
        [['c', 'C'], 1],
        // 'd' is missing in second stream
      ])
      secondInput.sendData(v([1, 0]), second)
      secondInput.sendFrontier(new Antichain([v([1, 0])]))

      // Send data to third input stream
      const third = new MultiSet<[string, boolean]>([
        [['a', true], 1],
        [['b', false], 1],
        // 'c' is missing in third stream
        [['d', true], 1],
      ])
      thirdInput.sendData(v([1, 0]), third)
      thirdInput.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph to compute the join
      await graph.run()

      // Verify results - with left join, all keys from first stream should be included
      expect(messages.length).toBe(1)

      // Check first version ('t1')
      const leftJoinData = messages[0].collection.getInner()

      // Expect 4 distinct tuples (a, b, c & d)
      expect(leftJoinData.length).toBe(4)

      // Verify the values
      const leftJoinSortedData = Array.from(leftJoinData).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      // Check 'a'
      expect(leftJoinSortedData[0][0][0]).toBe('a')
      expect(leftJoinSortedData[0][0][1]).toEqual([1, 'A', true])

      // Check 'b'
      expect(leftJoinSortedData[1][0][0]).toBe('b')
      expect(leftJoinSortedData[1][0][1]).toEqual([2, 'B', false])

      // Check 'c' - missing in third stream
      expect(leftJoinSortedData[2][0][0]).toBe('c')
      expect(leftJoinSortedData[2][0][1]).toEqual([3, 'C', null])

      // Check 'd' - missing in second stream
      expect(leftJoinSortedData[3][0][0]).toBe('d')
      expect(leftJoinSortedData[3][0][1]).toEqual([4, null, true])
    })

    it('should handle empty array of other streams', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create input stream
      const input = graph.newInput<[string, number]>()

      // Store messages for later verification
      const messages: DataMessage<[string, number[]]>[] = []

      // Join with empty array of other streams
      input.pipe(
        joinAll([], dbWrapper),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to input stream
      const data = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
        [['c', 3], 1],
      ])
      input.sendData(v([1, 0]), data)
      input.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph
      await graph.run()

      // Check results - should be original values wrapped in arrays
      expect(messages.length).toBe(1)

      const emptyJoinData = messages[0].collection.getInner()

      // Expect 3 distinct tuples (a, b & c)
      expect(emptyJoinData.length).toBe(3)

      // Verify the values
      const emptyJoinSortedData = Array.from(emptyJoinData).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      expect(emptyJoinSortedData[0][0][0]).toBe('a')
      expect(emptyJoinSortedData[0][0][1]).toEqual([1])
      expect(emptyJoinSortedData[0][1]).toBe(1)

      expect(emptyJoinSortedData[1][0][0]).toBe('b')
      expect(emptyJoinSortedData[1][0][1]).toEqual([2])
      expect(emptyJoinSortedData[1][1]).toBe(1)

      expect(emptyJoinSortedData[2][0][0]).toBe('c')
      expect(emptyJoinSortedData[2][0][1]).toEqual([3])
      expect(emptyJoinSortedData[2][1]).toBe(1)
    })

    it('should join streams across multiple versions', async () => {
      // Create a new graph
      const graph = new D2({ initialFrontier: v([0, 0]) })

      // Create input streams
      const firstInput = graph.newInput<[string, number]>()
      const secondInput = graph.newInput<[string, string]>()

      // Store messages for later verification
      const messages: DataMessage<[string, (number | string | null)[]]>[] = []

      // Join the streams
      firstInput.pipe(
        joinAll([secondInput], dbWrapper),
        output((message) => {
          if (message.type === MessageType.DATA) {
            messages.push(message.data)
          }
        }),
      )

      graph.finalize()

      // Send data to first input stream in version 1
      const first1 = new MultiSet<[string, number]>([
        [['a', 1], 1],
        [['b', 2], 1],
      ])
      firstInput.sendData(v([1, 0]), first1)

      // Send data to second input stream in version 1
      const second1 = new MultiSet<[string, string]>([
        [['a', 'A'], 1],
        [['c', 'C'], 1],
      ])
      secondInput.sendData(v([1, 0]), second1)

      // Update frontiers to version 1
      firstInput.sendFrontier(new Antichain([v([1, 0])]))
      secondInput.sendFrontier(new Antichain([v([1, 0])]))

      // Run the graph for version 1
      await graph.run()

      // Send data to first input stream in version 2
      const first2 = new MultiSet<[string, number]>([
        [['a', 10], 1], // Updated value
        [['b', 2], -1], // Remove b
        [['d', 4], 1], // Add d
      ])
      firstInput.sendData(v([2, 0]), first2)

      // Send data to second input stream in version 2
      const second2 = new MultiSet<[string, string]>([
        [['b', 'B'], 1], // Add b (but too late, b already removed from first)
        [['c', 'C'], -1], // Remove c
        [['d', 'D'], 1], // Add d
      ])
      secondInput.sendData(v([2, 0]), second2)

      // Update frontiers to version 2
      firstInput.sendFrontier(new Antichain([v([2, 0])]))
      secondInput.sendFrontier(new Antichain([v([2, 0])]))

      // Run the graph for version 2
      await graph.run()

      // Check we have both versions
      expect(messages.length).toBe(2)

      // Verify version 1 results
      const v1Data = messages[0].collection.getInner()
      expect(v1Data.length).toBe(1) // Only 'a' is in both streams

      // Verify 'a' in version 1
      const t1Entries = Array.from(v1Data).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )
      expect(t1Entries[0][0][0]).toBe('a')
      expect(t1Entries[0][0][1]).toEqual([1, 'A'])

      // Verify version 2 results - we should have updated values
      const v2Data = messages[1].collection.getInner()
      // Should have 'd' in version 2, 'a' from version 1 will be updated, 'b' will be removed
      expect(v2Data.length).toBe(3) // 'a' and 'd' and possibly 'b'

      // Convert to array and sort for consistent comparison
      const t2Entries = Array.from(v2Data).sort((a, b) =>
        a[0][0].localeCompare(b[0][0]),
      )

      // Check 'a' has updated value
      expect(t2Entries[0][0][0]).toBe('a')
      expect(t2Entries[0][0][1]).toContain(10)
      expect(t2Entries[0][0][1]).toContain('A')

      // Check 'b' is still present
      expect(t2Entries[1][0][0]).toBe('b')

      // Check 'd' was added
      expect(t2Entries[2][0][0]).toBe('d')
      expect(t2Entries[2][0][1]).toContain(4)
      expect(t2Entries[2][0][1]).toContain('D')
    })
  })
})
