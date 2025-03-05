# @electric-sql/d2ts

## 0.1.4

### Patch Changes

- 434996c: Add support for wildcard select with a `@*` and `@table_name.*` syntax
- 24451fc: add support for `group by` clause to D2QL
- 0e98a5a: new `groupBy` operator with accompanying aggregate functions (`sum`, `count`, `avg`, `min`, `max`, `median`, `mode`)
- 31d001d: fix the join implementation so that it returns current results

## 0.1.3

### Patch Changes

- 7648e99: ElectricSQL intigration
- 841438e: Initial implementation of D2QL and experimental SQL-like query language for D2TS
- 4a71e5b: A new keyBy, unkey, and rekey operators that simplify the keying process
- 6fcaf3b: New option to the join operator to specify the join type (inner, left, right, full)

## 0.1.2

### Patch Changes

- e7c004e: Fix an issue with the version-index where an append to it would have no affect

## 0.1.1

### Patch Changes

- b2bab2b: Update the package metadata
