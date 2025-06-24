---
'@electric-sql/d2mini': patch
---

Introduce topKWithFractionalIndexBTree and orderByWithFractionalIndexBTree operators. These variants use a B+ tree which should be efficient for big collections (logarithmic time).
