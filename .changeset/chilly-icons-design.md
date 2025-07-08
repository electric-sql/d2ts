---
'@electric-sql/d2mini': patch
---

Introduce topKWithFractionalIndexBTree and orderByWithFractionalIndexBTree operators. These variants use a B+ tree which is more efficient on big collections as its time complexity is logarithmic.
