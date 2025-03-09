import { keyBy } from '../operators/keying'
import { IStreamBuilder } from '../types'
import { Query } from './schema'

export function processKeyBy(
  resultPipeline: IStreamBuilder<
    Record<string, unknown> | [string | number, Record<string, unknown>]
  >,
  query: Query,
) {
  if (!query.keyBy) {
    return resultPipeline
  }
  const keyByParam = query.keyBy

  resultPipeline = resultPipeline.pipe(
    keyBy((row: Record<string, unknown>) => {
      if (Array.isArray(keyByParam)) {
        // Multiple columns - extract values and JSON stringify
        const keyValues: Record<string, unknown> = {}
        for (const keyColumn of keyByParam) {
          // Remove @ prefix if present
          const columnName = keyColumn.startsWith('@')
            ? keyColumn.substring(1)
            : keyColumn

          if (columnName in row) {
            keyValues[columnName] = row[columnName]
          } else {
            throw new Error(
              `Key column "${columnName}" not found in result set. Make sure it's included in the select clause.`,
            )
          }
        }
        return JSON.stringify(keyValues)
      } else {
        // Single column
        // Remove @ prefix if present
        const columnName = keyByParam.startsWith('@')
          ? keyByParam.substring(1)
          : keyByParam

        if (!(columnName in row)) {
          throw new Error(
            `Key column "${columnName}" not found in result set. Make sure it's included in the select clause.`,
          )
        }

        const keyValue = row[columnName]
        // Use the value directly if it's a string or number, otherwise JSON stringify
        if (typeof keyValue === 'string' || typeof keyValue === 'number') {
          return keyValue
        } else {
          return JSON.stringify(keyValue)
        }
      }
    }),
  )

  return resultPipeline
}
