# @electric-sql/d2mini

## 0.1.6

### Patch Changes

- a9f40ad: Remove file that was pushed accidentally.

## 0.1.5

### Patch Changes

- c5a59cf: Introduce topKWithFractionalIndexBTree and orderByWithFractionalIndexBTree operators. These variants use a B+ tree which is more efficient on big collections as its time complexity is logarithmic.
- 47e33ab: Modify index implementation to keep a map of consolidated values and their multiplicities. This improves efficiency to get a value's multiplicity since it's already precomputed. Also modify reduce operator to emit a single diff instead of 2 diffs (1 that is -oldMultiplicity and 1 that is +newMultiplicity).

## 0.1.4

### Patch Changes

- 159d963: fix an issue where messages could be lost if you sent multiple batches to a graph with a join operator before calling run

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
