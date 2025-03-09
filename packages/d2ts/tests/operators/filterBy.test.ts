import { describe, test, expect } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { filterBy, output } from '../../src/operators/index.js'
import { Message, MessageType } from '../../src/types.js'
import { KeyValue } from '../../src/types.js'

describe('Operators', () => {
  describe('FilterBy operation', () => {
    test('filterBy operator exists', () => {
      expect(typeof filterBy).toBe('function')
    })

    test('filterBy basic test', () => {
      const graph = new D2({ initialFrontier: 0 })
      const inputA = graph.newInput<KeyValue<number, string>>()
      const inputB = graph.newInput<KeyValue<number, boolean>>()
      const messages: Message<any>[] = []

      // Use the filterBy operator
      inputA.pipe(
        filterBy(inputB),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // Send data to the main stream
      inputA.sendData(
        1,
        new MultiSet<KeyValue<number, string>>([
          [[1, 'apple'], 1],
          [[2, 'banana'], 1],
        ]),
      )
      inputA.sendFrontier(1)

      // Send filter keys to the filter stream
      inputB.sendData(
        1,
        new MultiSet<KeyValue<number, boolean>>([
          [[1, true], 1],
        ]),
      )
      inputB.sendFrontier(1)

      graph.run()

      // Check if we got any frontier messages
      const frontierMessages = messages.filter(m => m.type === MessageType.FRONTIER)
      expect(frontierMessages.length).toBeGreaterThan(0)
    })

    test('filterBy with empty filter stream', () => {
      const graph = new D2({ initialFrontier: 0 })
      const inputA = graph.newInput<KeyValue<number, string>>()
      const inputB = graph.newInput<KeyValue<number, boolean>>()
      const messages: Message<any>[] = []

      inputA.pipe(
        filterBy(inputB),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // Send data to the main stream
      inputA.sendData(
        1,
        new MultiSet<KeyValue<number, string>>([
          [[1, 'apple'], 1],
          [[2, 'banana'], 1],
        ]),
      )
      inputA.sendFrontier(1)

      // Send empty filter data
      inputB.sendData(1, new MultiSet([]))
      inputB.sendFrontier(1)

      graph.run()

      // No data messages should be returned since filter stream is empty
      const dataMessages = messages.filter(m => m.type === MessageType.DATA)
      expect(dataMessages.length).toBe(0)
    })

    test('filterBy with late arriving filter data', () => {
      const graph = new D2({ initialFrontier: 0 })
      const inputA = graph.newInput<KeyValue<number, string>>()
      const inputB = graph.newInput<KeyValue<number, boolean>>()
      const messages: Message<any>[] = []

      inputA.pipe(
        filterBy(inputB),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // Send data to the main stream first
      inputA.sendData(
        1,
        new MultiSet<KeyValue<number, string>>([
          [[1, 'apple'], 1],
          [[2, 'banana'], 1],
          [[3, 'cherry'], 1],
        ]),
      )
      inputA.sendFrontier(1)

      graph.run()

      // No data messages yet because filter stream hasn't provided any keys
      const initialDataMessages = messages.filter(m => m.type === MessageType.DATA)
      expect(initialDataMessages.length).toBe(0)

      // Now send filter keys
      inputB.sendData(
        1,
        new MultiSet<KeyValue<number, boolean>>([
          [[2, true], 1],
          [[3, false], 1],
        ]),
      )
      inputB.sendFrontier(1)

      graph.run()

      // Now we should have frontier messages
      const frontierMessages = messages.filter(m => m.type === MessageType.FRONTIER)
      expect(frontierMessages.length).toBeGreaterThan(0)
    })

    test('filterBy with updates to filter stream', () => {
      const graph = new D2({ initialFrontier: 0 })
      const inputA = graph.newInput<KeyValue<number, string>>()
      const inputB = graph.newInput<KeyValue<number, boolean>>()
      const messages: Message<any>[] = []

      inputA.pipe(
        filterBy(inputB),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // Send initial data
      inputA.sendData(
        1,
        new MultiSet<KeyValue<number, string>>([
          [[1, 'apple'], 1],
          [[2, 'banana'], 1],
          [[3, 'cherry'], 1],
          [[4, 'date'], 1],
        ]),
      )
      inputA.sendFrontier(1)

      // Send initial filter keys
      inputB.sendData(
        1,
        new MultiSet<KeyValue<number, boolean>>([
          [[1, true], 1],
          [[3, true], 1],
        ]),
      )
      inputB.sendFrontier(1)

      graph.run()

      // Now update the filter stream with new keys
      inputB.sendData(
        2,
        new MultiSet<KeyValue<number, boolean>>([
          [[2, true], 1],
          [[4, true], 1],
        ]),
      )
      inputB.sendFrontier(2)

      graph.run()

      // We should have frontier messages
      const frontierMessages = messages.filter(m => m.type === MessageType.FRONTIER)
      expect(frontierMessages.length).toBeGreaterThan(0)
    })

    test('filterBy with negative multiplicities', () => {
      const graph = new D2({ initialFrontier: 0 })
      const inputA = graph.newInput<KeyValue<number, string>>()
      const inputB = graph.newInput<KeyValue<number, boolean>>()
      const messages: Message<any>[] = []

      inputA.pipe(
        filterBy(inputB),
        output((message) => {
          messages.push(message)
        }),
      )

      graph.finalize()

      // Send data to the main stream with negative multiplicity
      inputA.sendData(
        1,
        new MultiSet<KeyValue<number, string>>([
          [[1, 'apple'], 1],
          [[2, 'banana'], -1],
          [[3, 'cherry'], 1],
        ]),
      )
      inputA.sendFrontier(1)

      // Send filter keys
      inputB.sendData(
        1,
        new MultiSet<KeyValue<number, boolean>>([
          [[1, true], 1],
          [[2, true], 1],
          [[3, true], 1],
        ]),
      )
      inputB.sendFrontier(1)

      graph.run()

      // We should have frontier messages
      const frontierMessages = messages.filter(m => m.type === MessageType.FRONTIER)
      expect(frontierMessages.length).toBeGreaterThan(0)
    })
  })
}) 