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
})
