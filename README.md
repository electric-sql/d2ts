# Differential Dataflow in TypeScript

A TypeScript implementation of [differential dataflow](https://github.com/MaterializeInc/differential)

## Overview

Differential dataflow is a powerful data-parallel programming framework that enables incremental computations over changing input data. This implementation provides:

- Core differential dataflow operators (map, filter, join, reduce, etc.)
- Support for iterative computations
- Incremental updates with partially ordered versions
- Optional SQLite backend for state management and restartability

## Key Features

- **Incremental Processing**: Efficiently process changes to input data without recomputing everything
- **Rich Operators**: Supports common operations with a pipeline API:
  - `concat()`: Concatenate two streams
  - `consolidate()`: Consolidates the elements in the stream at each version
  - `count()`: Count elements by key
  - `distinct()`: Remove duplicates
  - `filter()`: Filter elements based on predicates
  - `iterate()`: Perform iterative computations
  - `join()`: Join two streams
  - `map()`: Transform elements in a stream
  - `reduce()`: Aggregate values by key
  - `output()`: Output the messages of the stream
  - `pipe()`: Build a pipeline of operators enabling reuse of combinations of operators
- **SQLite Integration**: Optional SQLite backend for managing operator state
- **Type Safety**: Full TypeScript type safety and inference through the pipeline API

## Quick Start

### Installation

```bash
npm install {TODO}
```

### Basic Usage

Here's a simple example that demonstrates the core concepts:

```typescript
import { D2, map, filter, debug, MultiSet, v } from 'd2ts'

// Create a new D2 graph with initial frontier
const graph = new D2({ initialFrontier: 0 })

// Create an input stream
const input = graph.newInput<number>()

// Build a simple pipeline that:
// 1. Takes numbers as input
// 2. Adds 5 to each number
// 3. Filters to keep only even numbers
const output = input.pipe(
  map(x => x + 5),
  filter(x => x % 2 === 0),
  debug('output')
)

// Finalize the graph
graph.finalize()

// Send some data
input.sendData(0), new MultiSet([
  [1, 1],
  [2, 1],
  [3, 1]
]))
input.sendFrontier(1)

// Process the data
graph.run()

// Output will show:
// 6 (from 1 + 5)
// 8 (from 3 + 5)
```

### Operators

#### `concat(other: IStreamBuilder<T>)`

Concatenates two input streams

```typescript
const output = input.pipe(
  concat(other)
)
```

#### `consolidate()`

Consolidates the elements in the stream at each version, essentially it ensures the output stream is at the latest known *complete* version.

```typescript
const output = input.pipe(
  consolidate()
)
```

#### `count()`

Counts the number of elements in the stream by key

```typescript
const output = input.pipe(
  map((data) => [data.somethingToKeyOn, data]),
  count()
)
```

#### `debug(name: string)`

Logs the messages of the stream to the console, the name is used to identify the stream in the logs.

```typescript
const output = input.pipe(
  debug('output')
)
```

#### `distinct()`

Removes duplicate values from the stream

```typescript
const output = input.pipe(
  distinct()
)
```

#### `filter(predicate: (data: T) => boolean)`

Filters the stream based on a predicate

```typescript
const output = input.pipe(
  filter(x => x % 2 === 0)
)
```

#### `iterate(f: (data: T) => T, initial: T)`

Performs an iterative computation on the stream

TODO: Explain and add example

#### `join(other: IStreamBuilder<T>)`

Joins two keyed streams, the output stream will contain the elements of the two streams combined, with the key of the element from the left stream.

This is an inner join, so only elements with matching keys will be included in the output.

```typescript
const input = graph.newInput<{ key: string, value: number }>()
const other = graph.newInput<{ key: string, value: string }>()

const output = input.pipe(
  join(other)
)
```

TODO: Add links to other joins when we have them

#### `map(f: (data: T) => T)`

Transforms the elements of the stream using a function

```typescript
const output = input.pipe(
  map(x => x + 5)
)
```

#### `output(messageHandler: (message: Message<T>) => void)`

Outputs the messages of the stream

TODO: expand on the Message type and how it works

```typescript
const output = input.pipe(
  output((message) => {
    console.log(message)
  })
)
```

#### `pipe(operator: (stream: IStreamBuilder<T>) => IStreamBuilder<T>)`

Pipes the stream through a series of operators

```typescript
const composedPipeline = pipe(
  map(x => x + 5),
  filter(x => x % 2 === 0),
  debug('output')
)

const output = input.pipe(
  composedPipeline
)

// Or as a function

const myPipe = (a: number, b: number) => pipe(
  map(x => x + a),
  filter(x => x % b === 0),
  debug('output')
)

const output = input.pipe(
  myPipe(5, 2)
)
```

#### `reduce(f: (values: [T, multiplicity: number][]) => [R, multiplicity: number][])`

Performs a reduce operation on the stream grouped by key.

The function `f` takes an array of values and their multiplicities and returns an array of the result and their multiplicities.

```typescript
// Count the number of elements in the stream by key
const output = input.pipe(
  map((data) => [data.somethingToKeyOn, data]),
  reduce((values) => values.map(([value, multiplicity]) => {
    let count = 0
    for (const [num, multiplicity] of values) {
      count += num * multiplicity
    }
    return [[count, 1]]
  }))
)
```

### Using SQLite Backend

For persistence and larger datasets, a number of operators are provided that persist to SQLite:

- `consolidate()`: Consolidates data into a single version
- `count()`: Counts the number of elements in a collection
- `distinct()`: Removes duplicates from a collection
- `join()`: Joins two collections
- `map()`: Transforms elements
- `reduce()`: Aggregates values by key

Each take a SQLite database as the final argument.

## Implementation Details

The implementation is based on the the one outlined in the [Materialize blog post](https://materialize.com/blog/differential-from-scratch/), with some TypeScript-specific adaptations, along with using a pipeline rather than builder api pattern.

1. Core data structures:

   - `MultiSet`: Represents collections with multiplicities
   - `Version`: Handles partially ordered versions
   - `Antichain`: Manages frontiers
   - `Index`: Stores versioned operator state

2. Operators:

   - Base operator classes in `src/operators/`
   - SQLite variants in `src/sqlite/operators/`

3. Graph execution:
   - Dataflow graph management in `src/graph.ts` and `src/D2.ts`
   - Message passing between operators
   - Frontier tracking and advancement

## References

- [Differential Dataflow](https://github.com/MaterializeInc/differential)
- [Differential Dataflow from Scratch](https://materialize.com/blog/differential-from-scratch/)
- [Python Implementation](https://github.com/ruchirK/python-differential)
