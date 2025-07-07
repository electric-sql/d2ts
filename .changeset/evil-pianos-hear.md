---
'@electric-sql/d2mini': patch
---

Modify index implementation to keep a map of consolidated values and their multiplicities. This improves efficiency to get a value's multiplicity since it's already precomputed. Also modify reduce operator to emit a single diff instead of 2 diffs (1 that is -oldMultiplicity and 1 that is +newMultiplicity).
