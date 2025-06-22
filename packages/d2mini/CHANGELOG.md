# @electric-sql/d2mini

## 0.1.3

### Patch Changes

- fb90328: fix a bug where `reduce` would not emit an message for deleted keys
- ef3829b: fix a bug where groupBy would not remove a group if it's key was completely removed from the stream

## 0.1.2

### Patch Changes

- 32d3a0f: make aggregates explicitly optional on a groupBy

## 0.1.1

### Patch Changes

- 080c0f4: First release of D2mini - a minimal implementation of the D2TS Differential Dataflow library but simplified and without the complexities of multi-dimensional versioning.
