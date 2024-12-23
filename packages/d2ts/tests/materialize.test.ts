import { describe, it, expect } from 'vitest'
import { D2 } from '../src/d2'
import { MultiSet } from '../src/multiset'
import { map } from '../src/operators'
import { v } from '../src/order'
import { Store } from '../src/store'

describe('StreamBuilder.materialize', () => {
  it('should create a store with initial data', () => {
    const graph = new D2({ initialFrontier: v(0) })
    const input = graph.newInput<{ key: string; value: number }>()

    const materialized = Store.materialize(
      input.pipe(map((data) => [data.key, data.value] as [string, number])),
    )

    graph.finalize()

    input.sendData(
      v(0),
      new MultiSet([
        [{ key: 'a', value: 1 }, 1],
        [{ key: 'b', value: 2 }, 1],
      ]),
    )

    input.sendFrontier(v(1))
    graph.step()

    expect(materialized.get('a')).toBe(1)
    expect(materialized.get('b')).toBe(2)
    expect(materialized.size).toBe(2)
  })

  it('should update store when data changes', () => {
    const graph = new D2({ initialFrontier: v(0) })
    const input = graph.newInput<{ key: string; value: number }>()

    const materialized = Store.materialize(
      input.pipe(map((data) => [data.key, data.value] as [string, number])),
    )

    graph.finalize()

    // Initial data
    input.sendData(v(0), new MultiSet([[{ key: 'a', value: 1 }, 1]]))
    input.sendFrontier(v(1))
    graph.step()

    // Update data
    input.sendData(
      v(1),
      new MultiSet([
        [{ key: 'a', value: 1 }, -1], // Remove old value
        [{ key: 'a', value: 10 }, 1], // Add new value
      ]),
    )
    input.sendFrontier(v(2))
    graph.step()

    expect(materialized.get('a')).toBe(10)
    expect(materialized.size).toBe(1)
  })

  it('should remove entries when they are deleted', () => {
    const graph = new D2({ initialFrontier: v(0) })
    const input = graph.newInput<{ key: string; value: number }>()

    const materialized = Store.materialize(
      input.pipe(map((data) => [data.key, data.value] as [string, number])),
    )

    graph.finalize()

    // Initial data
    input.sendData(
      v(0),
      new MultiSet([
        [{ key: 'a', value: 1 }, 1],
        [{ key: 'b', value: 2 }, 1],
      ]),
    )
    input.sendFrontier(v(1))
    graph.step()

    // Delete entry
    input.sendData(v(1), new MultiSet([[{ key: 'a', value: 1 }, -1]]))
    input.sendFrontier(v(2))
    graph.step()

    expect(materialized.has('a')).toBe(false)
    expect(materialized.get('b')).toBe(2)
    expect(materialized.size).toBe(1)
  })

  it('should emit change events when store is updated', () => {
    const graph = new D2({ initialFrontier: v(0) })
    const input = graph.newInput<{ key: string; value: number }>()

    const materialized = Store.materialize(
      input.pipe(map((data) => [data.key, data.value] as [string, number])),
    )

    graph.finalize()

    const changes: any[] = []
    materialized.addEventListener('change', ((e: CustomEvent) => {
      changes.push(e.detail)
    }) as EventListener)

    // Initial data
    input.sendData(v(0), new MultiSet([[{ key: 'a', value: 1 }, 1]]))
    input.sendFrontier(v(1))
    graph.step()

    expect(changes).toHaveLength(1)
    expect(changes[0]).toEqual([
      {
        type: 'insert',
        key: 'a',
        value: 1,
      },
    ])

    // Update data
    input.sendData(
      v(1),
      new MultiSet([
        [{ key: 'a', value: 1 }, -1],
        [{ key: 'a', value: 10 }, 1],
      ]),
    )
    input.sendFrontier(v(2))
    graph.step()

    expect(changes).toHaveLength(2)
    expect(changes[1]).toEqual([
      {
        type: 'update',
        key: 'a',
        value: 10,
        previousValue: 1,
      },
    ])
  })

  it('should handle multiple updates in a single step', () => {
    const graph = new D2({ initialFrontier: v(0) })
    const input = graph.newInput<{ key: string; value: number }>()

    const materialized = Store.materialize(
      input.pipe(map((data) => [data.key, data.value] as [string, number])),
    )

    graph.finalize()

    input.sendData(
      v(0),
      new MultiSet([
        [{ key: 'a', value: 1 }, 1],
        [{ key: 'b', value: 2 }, 1],
        [{ key: 'c', value: 3 }, 1],
        [{ key: 'a', value: 1 }, -1],
        [{ key: 'a', value: 10 }, 1],
      ]),
    )
    input.sendFrontier(v(1))
    graph.step()

    expect(materialized.get('a')).toBe(10)
    expect(materialized.get('b')).toBe(2)
    expect(materialized.get('c')).toBe(3)
    expect(materialized.size).toBe(3)
  })
})
