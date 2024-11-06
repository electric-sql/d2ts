import { describe, it, expect } from 'vitest'
import { v, Version, Antichain } from '../src/order'

describe('Version', () => {
  it('should handle version comparisons', () => {
    const v0_0 = new Version([0, 0])
    const v1_0 = new Version([1, 0])
    const v0_1 = new Version([0, 1])
    const v1_1 = new Version([1, 1])

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
    const v0_0 = new Version([0, 0])
    const v1_0 = new Version([1, 0])
    const v2_0 = new Version([2, 0])
    const v1_1 = new Version([1, 1])

    expect(new Antichain([v0_0]).lessEqual(new Antichain([v1_0]))).toBe(true)
    expect(new Antichain([v0_0]).equals(new Antichain([v1_0]))).toBe(false)
    expect(new Antichain([v0_0]).lessThan(new Antichain([v1_0]))).toBe(true)
    expect(new Antichain([v2_0, v1_1]).lessThan(new Antichain([v2_0]))).toBe(
      true,
    )
  })

  it('should check equals', () => {
    const v0_0 = new Version([0, 0])
    const v1_0 = new Version([1, 0])
    expect(v0_0.equals(v0_0)).toBe(true)
    expect(v0_0.equals(new Version([0, 0]))).toBe(true)
    expect(v0_0.equals(v1_0)).toBe(false)
  })

  it('should format version correctly', () => {
    expect(new Version([1, 2]).toString()).toBe('Version([1,2])')
    expect(new Version(5).toString()).toBe('Version([5])')
  })

  it('should join two versions', () => {
    const v1 = new Version([1, 2])
    const v2 = new Version([2, 3])
    expect(v1.join(v2).getInner()).toEqual([2, 3])
  })

  it('should meet two versions', () => {
    const v1 = new Version([1, 2])
    const v2 = new Version([2, 1])
    expect(v1.meet(v2).getInner()).toEqual([1, 1])
  })

  it('should advance version by empty frontier', () => {
    const version = new Version([1, 2])
    const frontier = new Antichain([])
    expect(version.advanceBy(frontier)).toBe(version)
  })

  it('should advance version by frontier', () => {
    const version = new Version([3, 1])
    const frontier = new Antichain([new Version([2, 3]), new Version([1, 3])])
    expect(version.advanceBy(frontier).getInner()).toEqual([3, 3])
  })

  it('should extend version with zero', () => {
    const version = new Version([1, 2])
    expect(version.extend().getInner()).toEqual([1, 2, 0])
  })

  it('should truncate last element', () => {
    const version = new Version([1, 2, 3])
    expect(version.truncate().getInner()).toEqual([1, 2])
  })

  it('should increment last element by step', () => {
    const version = new Version([1, 2])
    expect(version.applyStep(3).getInner()).toEqual([1, 5])
  })

  it('should throw error for non-positive step', () => {
    const version = new Version([1, 2])
    expect(() => version.applyStep(0)).toThrow('Step must be positive')
    expect(() => version.applyStep(-1)).toThrow('Step must be positive')
  })

  it('should throw error for negative numbers', () => {
    expect(() => new Version([-1])).toThrow(
      'Version numbers must be non-negative integers',
    )
    expect(() => new Version([1, -2])).toThrow(
      'Version numbers must be non-negative integers',
    )
  })

  it('should throw error for non-integer numbers', () => {
    expect(() => new Version([1.5])).toThrow(
      'Version numbers must be non-negative integers',
    )
    expect(() => new Version([1, 2.3])).toThrow(
      'Version numbers must be non-negative integers',
    )
  })

  it('should throw error when comparing versions of different dimensions', () => {
    const v1 = new Version([1, 2])
    const v2 = new Version([1, 2, 3])
    expect(() => v1.lessEqual(v2)).toThrow('Version dimensions must match')
    expect(() => v1.join(v2)).toThrow('Version dimensions must match')
    expect(() => v1.meet(v2)).toThrow('Version dimensions must match')
  })
})

describe('v factory', () => {
  it('should cache Version instances', () => {
    const v1 = v([1, 0])
    const v2 = v([1, 0])
    expect(v1).toBe(v2) // Test object identity (same reference)
  })

  it('should accept single numbers', () => {
    const version = v(1)
    expect(version.getInner()).toEqual([1])
  })

  it('should accept number arrays', () => {
    const version = v([1, 2, 3])
    expect(version.getInner()).toEqual([1, 2, 3])
  })
})

describe('Antichain', () => {
  it('should construct and maintain minimal elements', () => {
    const a1 = new Version([1, 0])
    const a2 = new Version([0, 1])
    const a3 = new Version([1, 1])

    // a3 dominates both a1 and a2
    const chain = new Antichain([a1, a2, a3])
    expect(chain.elements.length).toBe(2)
    expect(chain.elements).toEqual(expect.arrayContaining([a1, a2]))
  })

  it('should handle meet operation', () => {
    const chain1 = new Antichain([v([1, 0]), v([0, 1])])
    const chain2 = new Antichain([v([1, 1])])
    const result = chain1.meet(chain2)

    expect(result.elements.length).toBe(2)
    expect(result.elements).toEqual(
      expect.arrayContaining([v([1, 0]), v([0, 1])]),
    )
  })

  it('should check if antichain is empty', () => {
    expect(new Antichain([]).isEmpty()).toBe(true)
    expect(new Antichain([v([1, 0])]).isEmpty()).toBe(false)
  })

  it('should compare with version using lessEqualVersion', () => {
    const chain = new Antichain([v([1, 0]), v([0, 1])])
    expect(chain.lessEqualVersion(v([1, 1]))).toBe(true)
    expect(chain.lessEqualVersion(v([0, 0]))).toBe(false)
  })

  it('should handle extend operation', () => {
    const chain = new Antichain([v([1, 0]), v([0, 1])])
    const extended = chain.extend()
    expect(extended.elements).toEqual(
      expect.arrayContaining([v([1, 0, 0]), v([0, 1, 0])]),
    )
  })

  it('should handle truncate operation', () => {
    const chain = new Antichain([v([1, 0, 0]), v([0, 1, 0])])
    const truncated = chain.truncate()
    expect(truncated.elements).toEqual(
      expect.arrayContaining([v([1, 0]), v([0, 1])]),
    )
  })

  it('should handle applyStep operation', () => {
    const chain = new Antichain([v([1, 0]), v([0, 1])])
    const stepped = chain.applyStep(2)
    expect(stepped.elements).toEqual(
      expect.arrayContaining([v([1, 2]), v([0, 3])]),
    )
  })

  it('should format antichain correctly', () => {
    const chain = new Antichain([v([1, 0]), v([0, 1])])
    expect(chain.toString()).toBe('Antichain([[1,0],[0,1]])')
  })
})
