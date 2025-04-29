import { describe, it, expect } from 'vitest'
import { queryBuilder } from '../../../src/d2ql/query-builder/query-builder.js'
import { Schema, Input } from '../../../src/d2ql/types.js'

// Test schema
interface Employee extends Input {
  id: number
  name: string
  department_id: number | null
}

interface Department extends Input {
  id: number
  name: string
  budget: number
}

// Make sure TestSchema extends Schema
interface TestSchema extends Schema {
  employees: Employee
  departments: Department
}

describe('QueryBuilder.with', () => {
  it('defines a simple CTE correctly', () => {
    const query = queryBuilder<TestSchema>()
      .with('emp_cte', (q) => q.from('employees').select('@id', '@name'))
      // We still need type assertions, but the outer schema knows about the CTE
      .from('emp_cte')
      .select('@id' as any, '@name' as any)

    const builtQuery = query.buildQuery()

    expect(builtQuery.with).toBeDefined()
    expect(builtQuery.with).toHaveLength(1)
    expect(builtQuery.with?.[0].as).toBe('emp_cte')
    expect(builtQuery.with?.[0].from).toBe('employees')
    expect(builtQuery.with?.[0].select).toHaveLength(2)
    expect(builtQuery.from).toBe('emp_cte')
  })

  it('defines multiple CTEs correctly', () => {
    const query = queryBuilder<TestSchema>()
      .with('emp_cte', (q) =>
        q.from('employees').select('@id', '@name', '@department_id'),
      )
      .with('dept_cte', (q) => q.from('departments').select('@id', '@name'))
      .from('emp_cte' as any)
      .join({
        type: 'inner',
        from: 'dept_cte' as any,
        on: ['@emp_cte.department_id', '=', '@dept_cte.id'] as any,
      })
      .select(
        '@emp_cte.id' as any,
        '@emp_cte.name' as any,
        '@dept_cte.name' as any,
      )

    const builtQuery = query.buildQuery()

    expect(builtQuery.with).toBeDefined()
    expect(builtQuery.with).toHaveLength(2)
    expect(builtQuery.with?.[0].as).toBe('emp_cte')
    expect(builtQuery.with?.[1].as).toBe('dept_cte')
    expect(builtQuery.from).toBe('emp_cte')
    expect(builtQuery.join).toBeDefined()
    expect(builtQuery.join?.[0].from).toBe('dept_cte')
  })

  it('allows chaining other methods after with', () => {
    const query = queryBuilder<TestSchema>()
      .with('filtered_employees', (q) =>
        q
          .from('employees')
          .where('@department_id', '=', 1)
          .select('@id', '@name'),
      )
      .from('filtered_employees' as any)
      .where('@id' as any, '>', 100)
      .select('@id' as any, { employee_name: '@name' as any })

    const builtQuery = query.buildQuery()

    expect(builtQuery.with).toBeDefined()
    expect(builtQuery.with?.[0].where).toBeDefined()
    expect(builtQuery.from).toBe('filtered_employees')
    expect(builtQuery.where).toBeDefined()
    expect(builtQuery.select).toHaveLength(2)
  })
})
