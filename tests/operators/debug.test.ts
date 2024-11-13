import { describe, test, expect, vi } from 'vitest'
import { GraphBuilder } from '../../src/builder'
import { MultiSet } from '../../src/multiset'
import { Antichain, v } from '../../src/order'

describe('Operators', () => {
  describe('Debug operation', () => {
    test('basic debug operation', () => {
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      const output = input.debug('test-multiple')

      const graph = graphBuilder.finalize()

      writer.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      writer.sendData(v([1, 0]), new MultiSet([[2, -1]]))
      writer.sendFrontier(new Antichain([v([1, 0])]))

      const consoleSpy = vi.spyOn(console, 'log')
      graph.step()

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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<number>()

      const output = input.debug('test-multiple')

      const graph = graphBuilder.finalize()

      writer.sendData(v([1, 0]), new MultiSet([[1, 1]]))
      writer.sendData(v([1, 0]), new MultiSet([[2, -1]]))
      writer.sendFrontier(new Antichain([v([1, 0])]))

      const consoleSpy = vi.spyOn(console, 'log')
      graph.step()

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
      const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))
      const [input, writer] = graphBuilder.newInput<[string, number]>()

      const output = input.debug('test-indent', true)

      const graph = graphBuilder.finalize()

      writer.sendData(
        v([1, 0]),
        new MultiSet([
          [['key1', 1], 1],
          [['key2', 2], 1],
        ]),
      )
      writer.sendFrontier(new Antichain([v([1, 0])]))

      const consoleSpy = vi.spyOn(console, 'log')
      graph.step()

      expect(consoleSpy).toHaveBeenCalledWith(
        'debug test-indent notification: frontier Antichain([[1,0]])',
      )
      consoleSpy.mockRestore()
    })
  })
})
