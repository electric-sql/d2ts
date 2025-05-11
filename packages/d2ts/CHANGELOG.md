# @electric-sql/d2ts

## 0.1.5

### Patch Changes

- 5a9a169: fix a bug in compaction that could result in missing message versions

## 0.1.4

### Patch Changes

- 481a3c7: Add filterBy operator to filter elements of a keyed stream by keys from another stream
- 546db4c: new orderBy and topK operators, with indexed and fractional indexed variants
- 250ae73: Support for providing the sqlite database to operators via dependency injection
- 0e98a5a: new `groupBy` operator with accompanying aggregate functions (`sum`, `count`, `avg`, `min`, `max`, `median`, `mode`)
- 31d001d: fix the join implementation so that it returns correct results

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
