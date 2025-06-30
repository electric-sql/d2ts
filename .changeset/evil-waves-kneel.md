---
'@electric-sql/d2mini': patch
---

fix an issue where messages could be lost if you sent multiple batches to a graph with a join operator before calling run
