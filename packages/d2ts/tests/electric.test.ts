import { describe, it, expect, vi, beforeEach } from 'vitest'
import { electricStreamToD2Input } from '../src/electric'
import { D2 } from '../src/d2'
import type { ShapeStreamInterface, Message, Offset } from '@electric-sql/client'

describe('electricStreamToD2Input', () => {
  let mockStream: ShapeStreamInterface
  let mockSubscribeCallback: (messages: Message[]) => void
  let d2: D2
  let input: any

  beforeEach(() => {
    mockSubscribeCallback = vi.fn()
    mockStream = {
      subscribe: (callback) => {
        mockSubscribeCallback = callback
        return () => {} // Return unsubscribe function
      },
      unsubscribeAll: vi.fn(),
      isLoading: () => false,
      lastSyncedAt: () => Date.now(),
      lastSynced: () => 0,
      isConnected: () => true,
      isUpToDate: true,
      lastOffset: '0_0',
      shapeHandle: 'test-handle',
      error: undefined
    }

    d2 = new D2({ initialFrontier: 0 })
    input = d2.newInput()
    vi.spyOn(input, 'sendData')
    vi.spyOn(input, 'sendFrontier')
  })

  it('should handle insert operations correctly', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input
    })

    const messages: Message[] = [{
      headers: {
        operation: 'insert'
      },
      offset: '100_001',
      key: 'test-1',
      value: { id: 1, name: 'test' }
    }]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([
        [['test-1', { id: 1, name: 'test' }], 1]
      ])
    )
  })

  it('should handle update operations as delete + insert', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input
    })

    const messages: Message[] = [{
      headers: {
        operation: 'update'
      },
      offset: '100_001',
      key: 'test-1',
      value: { id: 1, name: 'updated' }
    }]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([
        [['test-1', { id: 1, name: 'updated' }], -1],
        [['test-1', { id: 1, name: 'updated' }], 1]
      ])
    )
  })

  it('should handle delete operations correctly', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input
    })

    const messages: Message[] = [{
      headers: {
        operation: 'delete'
      },
      offset: '100_001',
      key: 'test-1',
      value: { id: 1, name: 'deleted' }
    }]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([
        [['test-1', { id: 1, name: 'deleted' }], -1]
      ])
    )
  })

  it('should handle control messages and send frontier', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input
    })

    const messages: Message[] = [{
      headers: {
        operation: 'insert'
      },
      offset: '100_001',
      key: 'test-1',
      value: { id: 1 }
    }, {
      headers: {
        control: 'up-to-date'
      }
    }]

    mockSubscribeCallback(messages)

    expect(input.sendFrontier).toHaveBeenCalledWith(101)
  })

  it('should use custom lsnToVersion and lsnToFrontier functions', () => {
    const customLsnToVersion = (lsn: number) => lsn * 2
    const customLsnToFrontier = (lsn: number) => lsn * 3

    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
      lsnToVersion: customLsnToVersion,
      lsnToFrontier: customLsnToFrontier
    })

    const messages: Message[] = [{
      headers: {
        operation: 'insert'
      },
      offset: '100_001',
      key: 'test-1',
      value: { id: 1 }
    }, {
      headers: {
        control: 'up-to-date'
      }
    }]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      200, // 100 * 2
      expect.arrayContaining([
        [['test-1', { id: 1 }], 1]
      ])
    )
    expect(input.sendFrontier).toHaveBeenCalledWith(303) // (100 + 1) * 3
  })

  it('should throw error for invalid LSN format', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input
    })

    const messages: Message[] = [{
      headers: {
        operation: 'insert'
      },
      offset: 'invalid_lsn' as unknown as Offset,
      key: 'test-1',
      value: { id: 1 }
    }]

    expect(() => mockSubscribeCallback(messages)).toThrow('Invalid LSN format: invalid_lsn')
  })
}) 