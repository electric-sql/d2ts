# Differential Dataflow in TypeScript

A TypeScript implementation of [differential dataflow](https://github.com/MaterializeInc/differential)

## Overview

Differential dataflow is a powerful data-parallel programming framework that enables incremental computations over changing input data. This implementation provides:

- Core differential dataflow operators (map, filter, join, reduce, etc.)
- Support for iterative computations
- Incremental updates with partially ordered versions
- Optional SQLite backend for state management and reusability

## Key Features

- **Incremental Processing**: Efficiently process changes to input data without recomputing everything
- **Rich Operators**: Supports common operations like:
  - `map()`: Transform elements
  - `filter()`: Filter elements based on predicates
  - `join()`: Join two collections
  - `reduce()`: Aggregate values by key
  - `count()`: Count elements by key
  - `distinct()`: Remove duplicates
  - `iterate()`: Perform iterative computations
- **SQLite Integration**: Optional SQLite backend for managing operator state
- **Type Safety**: Full TypeScript type safety and inference

## Quick Start

### Installation

```bash
npm install {TODO}
```

### Basic Usage

Here's a simple example that demonstrates the core concepts:

```typescript
import { GraphBuilder } from 'differential-dataflow-ts'
import { MultiSet } from 'differential-dataflow-ts/multiset'
import { Antichain, v } from 'differential-dataflow-ts/order'

// Create a new graph builder with initial frontier
const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

// Create an input stream
const [input, writer] = graphBuilder.newInput<number>()

// Build a simple pipeline that:
// 1. Takes numbers as input
// 2. Adds 5 to each number
// 3. Filters to keep only even numbers
const output = input
  .map((x) => x + 5)
  .filter((x) => x % 2 === 0)
  .debug('output')

// Finalize the graph
const graph = graphBuilder.finalize()

// Send some data
writer.sendData(
  v([0, 0]),
  new MultiSet([
    [1, 1],
    [2, 1],
    [3, 1],
  ]),
)
writer.sendFrontier(new Antichain([v([0, 1])]))

// Process the data
graph.step()

// Output will show:
// 6 (from 1 + 5)
// 8 (from 3 + 5)
```

### Using SQLite Backend

For persistence and larger datasets, you can use the SQLite backend:

```typescript
import Database from 'better-sqlite3'
const db = new Database('myapp.sqlite')
const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]), db)
// ... rest of the code remains the same
```

### Key Concepts

1. **Versions**: Each piece of data has an associated version (timestamp)
2. **MultiSets**: Collections that track element counts (can be negative for deletions)
3. **Frontiers**: Track progress of computation through version space
4. **Incremental Updates**: Only recompute what's necessary when data changes

See the `examples/` directory for more complex scenarios including:

- Joins between datasets
- Iterative computations
- Graph processing
- Real-time updates

## Implementation Details

The implementation follows the structure outlined in the Materialize blog post, with some TypeScript-specific adaptations:

1. Core data structures:

   - `MultiSet`: Represents collections with multiplicities
   - `Version`: Handles partially ordered versions
   - `Antichain`: Manages frontiers
   - `Index`: Stores versioned operator state

2. Operators:

   - Base operator classes in `src/operators.ts`
   - SQLite variants in `src/operators-sqlite.ts`
   - Graph construction in `src/builder.ts`

3. Graph execution:
   - Dataflow graph management in `src/graph.ts`
   - Message passing between operators
   - Frontier tracking and advancement

## References

- [Differential Dataflow](https://github.com/MaterializeInc/differential)
- [Differential Dataflow from Scratch](https://materialize.com/blog/differential-from-scratch/)
- [Python Implementation](https://github.com/ruchirK/python-differential)
