import { describe, it, expect } from 'vitest'
import { evaluateFunction, isFunctionCall } from '../../src/d2ql/functions.js'

describe('D2QL > Functions', () => {
  describe('isFunctionCall', () => {
    it('identifies valid function calls', () => {
      expect(isFunctionCall({ UPPER: '@name' })).toBe(true)
      expect(isFunctionCall({ LOWER: '@description' })).toBe(true)
      expect(isFunctionCall({ LENGTH: '@text' })).toBe(true)
      expect(isFunctionCall({ DATE: '@dateColumn' })).toBe(true)
    })

    it('rejects invalid function calls', () => {
      expect(isFunctionCall(null)).toBe(false)
      expect(isFunctionCall(undefined)).toBe(false)
      expect(isFunctionCall('string')).toBe(false)
      expect(isFunctionCall(42)).toBe(false)
      expect(isFunctionCall({})).toBe(false)
      expect(isFunctionCall({ notAFunction: 'value' })).toBe(false)
      expect(isFunctionCall({ UPPER: '@name', LOWER: '@name' })).toBe(false) // Multiple keys
    })
  })

  describe('Function stubs', () => {
    it('throws "not implemented" for non-aggregate functions', () => {
      expect(() => evaluateFunction('UPPER', '@name')).toThrow(
        'not implemented',
      )
      expect(() => evaluateFunction('LOWER', '@name')).toThrow(
        'not implemented',
      )
      expect(() => evaluateFunction('LENGTH', '@name')).toThrow(
        'not implemented',
      )
      expect(() => evaluateFunction('DATE', '@date')).toThrow('not implemented')
      expect(() => evaluateFunction('CONCAT', ['@first', '@last'])).toThrow(
        'not implemented',
      )
      expect(() =>
        evaluateFunction('COALESCE', ['@value', null, 'default']),
      ).toThrow('not implemented')
      expect(() =>
        evaluateFunction('JSON_EXTRACT', ['@json', '$.path']),
      ).toThrow('not implemented')
    })
  })
})
