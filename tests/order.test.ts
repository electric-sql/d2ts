import { describe, it, expect } from 'vitest'
import { V, Antichain } from '../src/order'

describe('Version', () => {
  it('should handle version comparisons', () => {
    const v0_0 = V([0, 0])
    const v1_0 = V([1, 0])
    const v0_1 = V([0, 1])
    const v1_1 = V([1, 1])

    expect(v0_0.lessThan(v1_0)).toBe(true)
    expect(v0_0.lessThan(v0_1)).toBe(true)
    expect(v0_0.lessThan(v1_1)).toBe(true)
    expect(v0_0.lessEqual(v1_0)).toBe(true)
    expect(v0_0.lessEqual(v0_1)).toBe(true)
    expect(v0_0.lessEqual(v1_1)).toBe(true)

    expect(v1_0.lessThan(v1_0)).toBe(false)
    expect(v1_0.lessEqual(v1_0)).toBe(true)
    expect(v1_0.lessEqual(v0_1)).toBe(false)
    expect(v0_1.lessEqual(v1_0)).toBe(false)
    expect(v0_1.lessEqual(v1_1)).toBe(true)
    expect(v1_0.lessEqual(v1_1)).toBe(true)
    expect(v0_0.lessEqual(v1_1)).toBe(true)
  })

  it('should handle antichain comparisons', () => {
    const v0_0 = V([0, 0])
    const v1_0 = V([1, 0])
    const v2_0 = V([2, 0])
    const v1_1 = V([1, 1])

    expect(new Antichain([v0_0]).lessEqual(new Antichain([v1_0]))).toBe(true)
    expect(new Antichain([v0_0]).equals(new Antichain([v1_0]))).toBe(false)
    expect(new Antichain([v0_0]).lessThan(new Antichain([v1_0]))).toBe(true)
    expect(new Antichain([v2_0, v1_1]).lessThan(new Antichain([v2_0]))).toBe(
      true,
    )
  })
})
