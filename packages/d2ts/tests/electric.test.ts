import { describe, it, expect, vi, beforeEach } from 'vitest'
import { electricStreamToD2Input } from '../src/electric'
import { D2 } from '../src/d2'
import type {
  ShapeStreamInterface,
  Message,
  Offset,
} from '@electric-sql/client'

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
      error: undefined,
      hasStarted: () => true,
      forceDisconnectAndRefresh: vi.fn(),
    }

    d2 = new D2({ initialFrontier: 0 })
    input = d2.newInput()
    vi.spyOn(input, 'sendData')
    vi.spyOn(input, 'sendFrontier')
  })

  it('should handle insert operations correctly when message has last flag', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'insert',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1, name: 'test' },
      },
    ]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([[['test-1', { id: 1, name: 'test' }], 1]]),
    )
  })

  it('should handle update operations as delete + insert when message has last flag', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'update',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1, name: 'updated' },
      },
    ]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([
        [['test-1', { id: 1, name: 'updated' }], -1],
        [['test-1', { id: 1, name: 'updated' }], 1],
      ]),
    )
  })

  it('should handle delete operations correctly when message has last flag', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'delete',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1, name: 'deleted' },
      },
    ]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([[['test-1', { id: 1, name: 'deleted' }], -1]]),
    )
  })

  it('should handle operations with up-to-date control message', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'insert',
          lsn: 100,
          op_position: 1,
        },
        key: 'test-1',
        value: { id: 1, name: 'test' },
      },
      {
        headers: {
          control: 'up-to-date',
          global_last_seen_lsn: 100,
        },
      },
    ]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([[['test-1', { id: 1, name: 'test' }], 1]]),
    )
    expect(input.sendFrontier).toHaveBeenCalledWith(101)
  })

  it('should handle control messages and send frontier', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
    })

    const messages: Message[] = [
      {
        headers: {
          control: 'up-to-date',
          global_last_seen_lsn: 100,
        },
      },
    ]

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
      lsnToFrontier: customLsnToFrontier,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'insert',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1 },
      },
      {
        headers: {
          control: 'up-to-date',
          global_last_seen_lsn: 100,
        },
      },
    ]

    mockSubscribeCallback(messages)

    expect(input.sendData).toHaveBeenCalledWith(
      200, // 100 * 2
      expect.arrayContaining([[['test-1', { id: 1 }], 1]]),
    )
    expect(input.sendFrontier).toHaveBeenCalledWith(303) // (100 + 1) * 3
  })

  it('should handle partial updates correctly by merging old and new values', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'update',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1, name: 'updated', age: 30 },
        old_value: { name: 'old' }, // Only contains changed fields
      },
    ]

    mockSubscribeCallback(messages)

    // Should send a delete with the complete old value (merged from value and old_value)
    expect(input.sendData).toHaveBeenCalledWith(
      100,
      expect.arrayContaining([
        [['test-1', { id: 1, name: 'old', age: 30 }], -1],
        [['test-1', { id: 1, name: 'updated', age: 30 }], 1],
      ]),
    )
  })

  it('should throw error on must-refetch control message', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
    })

    const messages: Message[] = [
      {
        headers: {
          control: 'must-refetch',
        },
      },
    ]

    expect(() => mockSubscribeCallback(messages)).toThrow(
      'The server sent a "must-refetch" request, this is incompatible with a D2 pipeline and unresolvable. To handle this you will have to remove all state and start the pipeline again.',
    )
  })

  it('should run graph on up-to-date control message when runOn is up-to-date', () => {
    const runSpy = vi.spyOn(d2, 'run')
    
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
      runOn: 'up-to-date',
    })

    const messages: Message[] = [
      {
        headers: {
          control: 'up-to-date',
          global_last_seen_lsn: 100,
        },
      },
    ]

    mockSubscribeCallback(messages)
    expect(runSpy).toHaveBeenCalled()
  })

  it('should run graph on lsn advance when runOn is lsn-advance', () => {
    const runSpy = vi.spyOn(d2, 'run')
    
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
      runOn: 'lsn-advance',
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'insert',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1 },
      },
    ]

    mockSubscribeCallback(messages)
    expect(runSpy).toHaveBeenCalled()
  })

  it('should use initialLsn when provided', () => {
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
      initialLsn: 50,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'insert',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1 },
      },
    ]

    mockSubscribeCallback(messages)
    expect(input.sendData).toHaveBeenCalledWith(100, expect.any(Array))
    expect(input.sendFrontier).toHaveBeenCalledWith(101)
  })

  it('should handle debug logging when enabled', () => {
    const consoleSpy = vi.spyOn(console, 'log')
    
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
      debug: true,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'insert',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1 },
      },
    ]

    mockSubscribeCallback(messages)
    expect(consoleSpy).toHaveBeenCalledWith('subscribing to stream')
    expect(consoleSpy).toHaveBeenCalledWith('received 1 messages')
    expect(consoleSpy).toHaveBeenCalledWith('- change message: insert')
  })

  it('should handle custom debug logging function', () => {
    const customLogger = vi.fn()
    
    electricStreamToD2Input({
      graph: d2,
      stream: mockStream,
      input,
      debug: customLogger,
    })

    const messages: Message[] = [
      {
        headers: {
          operation: 'insert',
          lsn: 100,
          op_position: 1,
          last: true,
        },
        key: 'test-1',
        value: { id: 1 },
      },
    ]

    mockSubscribeCallback(messages)
    expect(customLogger).toHaveBeenCalledWith('subscribing to stream')
    expect(customLogger).toHaveBeenCalledWith('received 1 messages')
    expect(customLogger).toHaveBeenCalledWith('- change message: insert')
  })
})
