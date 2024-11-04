# Differential Dataflow in TypeScript

This is an implementation of differential dataflow in TypeScript, based on this [Python implementation](https://github.com/ruchirK/python-differential/tree/main).

Still a work in progress, with this to do:

- The `iterate` operator is broken.
- Fix the types.
- Investigate the performance, particularly arround using JSON.stringify for checking uniqueness.
- Improve the API design.
