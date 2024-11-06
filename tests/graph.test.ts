import { describe, expect, beforeEach, test } from 'vitest'
import { DifferenceStreamReader, DifferenceStreamWriter } from '../src/graph'
import { MessageType } from '../src/types'
import { v, Antichain } from '../src/order'
import { MultiSet } from '../src/multiset'

describe('DifferenceStreamReader and DifferenceStreamWriter', () => {
  let writer: DifferenceStreamWriter<number>
  let reader: DifferenceStreamReader<number>

  beforeEach(() => {
    writer = new DifferenceStreamWriter<number>()
    reader = writer.newReader()
  })

  test('isEmpty returns true for empty queue', () => {
    expect(reader.isEmpty()).toBe(true)
  })

  test('isEmpty returns false when queue has messages', () => {
    writer.sendData(v(1), new MultiSet())
    expect(reader.isEmpty()).toBe(false)
  })

  test('drain returns all messages', () => {
    writer.sendData(v(1), new MultiSet([[1, 1]]))
    writer.sendData(v(2), new MultiSet([[2, 1]]))

    const messages = reader.drain()
    expect(messages).toHaveLength(2)
    expect(messages[0].type).toBe(MessageType.DATA)
    expect(messages[1].type).toBe(MessageType.DATA)
    expect(reader.isEmpty()).toBe(true)
  })

  test('probeFrontierLessThan returns true when frontier is greater', () => {
    const frontier1 = new Antichain([v(1)])
    const frontier2 = new Antichain([v(2)])

    writer.sendFrontier(frontier1)
    expect(reader.probeFrontierLessThan(frontier2)).toBe(true)
  })

  test('probeFrontierLessThan returns false when frontier is less or equal', () => {
    const frontier1 = new Antichain([v(2)])
    const frontier2 = new Antichain([v(1)])

    writer.sendFrontier(frontier1)
    expect(reader.probeFrontierLessThan(frontier2)).toBe(false)
  })
})
