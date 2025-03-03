import { map, filter } from '../operators/index.js'
import { IStreamBuilder } from '../types.js'
import { evaluateConditionOnNestedRow } from './evaluators.js'

/**
 * Creates a processing pipeline for join results
 */
export function processJoinResults(
  mainTableAlias: string,
  joinedTableAlias: string,
  joinClause: { on: any; where?: any; type: string },
) {
  return function (
    pipeline: IStreamBuilder<unknown>,
  ): IStreamBuilder<Record<string, unknown>> {
    return pipeline.pipe(
      // Process the join result and handle nulls in the same step
      map((result: unknown) => {
        const [_key, [mainNestedRow, joinedNestedRow]] = result as [
          unknown,
          [
            Record<string, unknown> | undefined,
            Record<string, unknown> | undefined,
          ],
        ]

        // For inner joins, both sides should be non-null
        if (joinClause.type === 'inner' || joinClause.type === 'cross') {
          if (!mainNestedRow || !joinedNestedRow) {
            return undefined // Will be filtered out
          }
        }

        // For left joins, the main row must be non-null
        if (joinClause.type === 'left' && !mainNestedRow) {
          return undefined // Will be filtered out
        }

        // For right joins, the joined row must be non-null
        if (joinClause.type === 'right' && !joinedNestedRow) {
          return undefined // Will be filtered out
        }

        // Merge the nested rows
        const mergedNestedRow: Record<string, unknown> = {}

        // Add main row data if it exists
        if (mainNestedRow) {
          Object.entries(mainNestedRow).forEach(([tableAlias, tableData]) => {
            mergedNestedRow[tableAlias] = tableData
          })
        }

        // If we have a joined row, add it to the merged result
        if (joinedNestedRow) {
          Object.entries(joinedNestedRow).forEach(([tableAlias, tableData]) => {
            mergedNestedRow[tableAlias] = tableData
          })
        } else if (joinClause.type === 'left' || joinClause.type === 'full') {
          // For left or full joins, add the joined table with null data if missing
          mergedNestedRow[joinedTableAlias] = null
        }

        // For right or full joins, add the main table with null data if missing
        if (
          !mainNestedRow &&
          (joinClause.type === 'right' || joinClause.type === 'full')
        ) {
          mergedNestedRow[mainTableAlias] = null
        }

        return mergedNestedRow
      }),
      // Filter out undefined results
      filter(
        (value: unknown): value is Record<string, unknown> =>
          value !== undefined,
      ),
      // Process the ON condition
      filter((nestedRow: Record<string, unknown>) => {
        // If there's no ON condition, or it's a cross join, always return true
        if (!joinClause.on || joinClause.type === 'cross') {
          return true
        }

        // For LEFT JOIN, if the right side is null, we should include the row
        if (
          joinClause.type === 'left' &&
          nestedRow[joinedTableAlias] === null
        ) {
          return true
        }

        // For RIGHT JOIN, if the left side is null, we should include the row
        if (joinClause.type === 'right' && nestedRow[mainTableAlias] === null) {
          return true
        }

        // For FULL JOIN, if either side is null, we should include the row
        if (
          joinClause.type === 'full' &&
          (nestedRow[mainTableAlias] === null ||
            nestedRow[joinedTableAlias] === null)
        ) {
          return true
        }

        const result = evaluateConditionOnNestedRow(
          nestedRow,
          joinClause.on,
          mainTableAlias,
          joinedTableAlias,
        )
        return result
      }),
      // Process the WHERE clause for the join if it exists
      filter((nestedRow: Record<string, unknown>) => {
        if (!joinClause.where) {
          return true
        }

        const result = evaluateConditionOnNestedRow(
          nestedRow,
          joinClause.where,
          mainTableAlias,
          joinedTableAlias,
        )
        return result
      }),
    ) as IStreamBuilder<Record<string, unknown>>
  }
}
