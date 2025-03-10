import { describe, test, expect, vi } from 'vitest'
import { D2 } from '../../src/d2.js'
import { MultiSet } from '../../src/multiset.js'
import { Antichain, v } from '../../src/order.js'
import { debug } from '../../src/operators/index.js'

describe('Operators', () => {
  describe('Debug operation', () => {
    test('basic debug operation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()

      input.pipe(debug('test-multiple'))

      graph.finalize()

      input.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      input.sendData(v([1, 0]), new MultiSet([[2, -1]]))
      input.sendFrontier(new Antichain([v([1, 0])]))

      const consoleSpy = vi.spyOn(console, 'log')
      graph.run()

      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-multiple data: version: Version([1,0]) collection: MultiSet([[1,1]])',
      )
      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-multiple data: version: Version([1,0]) collection: MultiSet([[2,-1]])',
      )
      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-multiple notification: frontier Antichain([[1,0]])',
      )
      consoleSpy.mockRestore()
    })

    test('debug with multiple messages', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<number>()

      input.pipe(debug('test-multiple'))

      graph.finalize()

      input.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      input.sendData(v([1, 0]), new MultiSet([[2, -1]]))
      input.sendFrontier(new Antichain([v([1, 0])]))

      const consoleSpy = vi.spyOn(console, 'log')
      graph.run()

      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-multiple data: version: Version([1,0]) collection: MultiSet([[1,1]])',
      )
      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-multiple data: version: Version([1,0]) collection: MultiSet([[2,-1]])',
      )
      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-multiple notification: frontier Antichain([[1,0]])',
      )
      consoleSpy.mockRestore()
    })

    test('debug with indentation', () => {
      const graph = new D2({ initialFrontier: v([0, 0]) })
      const input = graph.newInput<[string, number]>()

      input.pipe(debug('test-indent', true))

      graph.finalize()

      input.sendData(
        v([1, 0]),
        new MultiSet([
          [['key1', 1], 1],
          [['key2', 2], 1],
        ]),
      )
      input.sendFrontier(new Antichain([v([1, 0])]))

      const consoleSpy = vi.spyOn(console, 'log')
      graph.run()

      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-indent notification: frontier Antichain([[1,0]])',
      )
      consoleSpy.mockRestore()
    })
  })
})
