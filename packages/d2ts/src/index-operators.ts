export type IndexOperator<K> = (key: K) => boolean

export const eq = <K>(matchKey: K): IndexOperator<K> => {
  return (key) => key === matchKey
}

export function neq<K>(matchKey: K): IndexOperator<K> {
  return (key) => key !== matchKey
}

export function gt<K>(matchKey: K): IndexOperator<K> {
  return (key) => key > matchKey
}

export function gte<K>(matchKey: K): IndexOperator<K> {
  return (key) => key >= matchKey
}

export function lt<K>(matchKey: K): IndexOperator<K> {
  return (key) => key < matchKey
}

export function lte<K>(matchKey: K): IndexOperator<K> {
  return (key) => key <= matchKey
}

export function isIn<K>(matchKeys: K[] | Set<K>): IndexOperator<K> {
  const matchKeysSet = matchKeys instanceof Set ? matchKeys : new Set(matchKeys)
  return (key) => {
    return matchKeysSet.has(key)
  }
}

export function between<K>(start: K, end: K): IndexOperator<K> {
  return (key) => key >= start && key <= end
}

export function and<K>(...operators: IndexOperator<K>[]): IndexOperator<K> {
  return (key) => operators.every((op) => op(key))
}

export function or<K>(...operators: IndexOperator<K>[]): IndexOperator<K> {
  return (key) => operators.some((op) => op(key))
}

export function not<K>(operator: IndexOperator<K>): IndexOperator<K> {
  return (key) => !operator(key)
}
