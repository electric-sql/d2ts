# D2TS - Differential Dataflow in TypeScript

D2TS is a TypeScript implementation of [differential dataflow](https://github.com/MaterializeInc/differential) - a powerful data-parallel programming framework that enables incremental computations over changing input data.

You can use D2TS to build data pipelines that can be executed incrementally, meaning you can process data as it comes in, and only recompute the parts that have changed. This could be as simple as remapping data, or as complex as performing a full join combining two datasources where one is a computed aggregate.

D2TS can be used in conjunction with [ElectricSQL](https://electric-sql.com) to build data pipelines on top os "Shape Streams" that can be executed incrementally.

A D2TS pipe is also fully type safe, inferring the types at each step of the pipeline, and supports auto-complete in your IDE.

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
npm install @electric-sql/d2ts
```

### Basic Usage

Here's a simple example that demonstrates the core concepts:

```typescript
import { D2, map, filter, debug, MultiSet, v } from '@electric-sql/d2ts'

// Create a new D2 graph with initial frontier
// The initial frontier is the lower bound of the version of the data that may
// come in future.
const graph = new D2({ initialFrontier: 0 })

// Create an input stream
// We can specify the type of the input stream, here we are using number.
const input = graph.newInput<number>()

// Build a simple pipeline that:
// 1. Takes numbers as input
// 2. Adds 5 to each number
// 3. Filters to keep only even numbers
// Pipelines can have multiple inputs and outputs.
const output = input.pipe(
  map((x) => x + 5),
  filter((x) => x % 2 === 0),
  debug('output'),
)

// Finalize the pipeline, after this point we can no longer add operators or
// inputs
graph.finalize()

// Send some data
// Data is sent as a MultiSet, which is a map of values to their multiplicity
// Here we are sending 3 numbers (1-3), each with a multiplicity of 1
// When you send data, you set the version number, here we are using 0
// The key thing to understand is that the MultiSet represents a *change* to
// the data, not the data itself. "Inserts" and "Deletes" are represented as
// an element with a multiplicity of 1 or -1 respectively.
input.sendData(
  0, // The version of the data
  new MultiSet([
    [1, 1],
    [2, 1],
    [3, 1],
  ]),
)

// Set the frontier to version 1
// The "frontier" is the lower bound of the version of the data that may come in future.
// By sending a frontier, you are indicating that you are done sending data for any version less than the frontier and therefor D2TS operators that require them can process that data and output the results.
input.sendFrontier(1)

// Process the data
graph.run()

// Output will show:
// 6 (from 1 + 5)
// 8 (from 3 + 5)
```

### MultiSet as a Change to a Collection

A `MultiSet` is a map of values to their multiplicity. It is used to represent the changes to a collection.

```typescript
// A MultiSet is created by passing an array of [value, multiplicity] pairs
// Here we are creating a MultiSet with the values 1, 2, and 3, each with a
// multiplicity of 1
const multiSet = new MultiSet([
  [1, 1],
  [2, 1],
  [3, 1],
])
```

MultiSets could be used to represent any object:

```typescript
// Here we have a MultiSet of new "comments" with the interface `Comment`
const multiSet = new MultiSet<Comment>([
  [{ id: '1', text: 'Hello, world!', userId: '321' }, 1],
  [{ id: '2', text: 'Hello, world!', userId: '123' }, 1],
])
```

An important principle of D2TS is "keyed" MultiSets, where the `value` is a tuple of `[key, value]`.

```typescript
// Here we have a MultiSet of new "comments" but we have keyed them by the
// `userId`
const multiSet = new MultiSet<[string, Comment]>([
  [['321', { id: '1', text: 'Hello, world!', userId: '321' }], 1],
  [['123', { id: '2', text: 'Hello, world!', userId: '123' }], 1],
])
```

Inserts and deletes are represented as an element with a multiplicity of 1 or -1 respectively.

```typescript
// Here we are inserting a one new comment and deleting one comment
const multiSet = new MultiSet<[string, Comment]>([
  [['321', { id: '1', text: 'Hello, world!', userId: '321' }], 1],
  [['123', { id: '2', text: 'Hello, world!', userId: '123' }], -1],
])
```

### Operators

#### `concat(other: IStreamBuilder<T>)`

Concatenates two input streams - the output stream will contain the elements of both streams.

```typescript
const output = input.pipe(concat(other))
```

#### `consolidate()`

Consolidates the elements in the stream at each version, essentially it ensures the output stream is at the latest known _complete_ version.

```typescript
const output = input.pipe(consolidate())
```

#### `count()`

Counts the number of elements in the stream by key

```typescript
const output = input.pipe(
  map((data) => [data.somethingToKeyOn, data]),
  count(),
)
```

#### `debug(name: string)`

Logs the messages of the stream to the console, the name is used to identify the stream in the logs.

```typescript
const output = input.pipe(debug('output'))
```

#### `distinct()`

Removes duplicate values from the stream by key

```typescript
const output = input.pipe(distinct())
```

#### `filter(predicate: (data: T) => boolean)`

Filters the stream based on a predicate

```typescript
const output = input.pipe(filter((x) => x % 2 === 0))
```

#### `iterate(f: (data: T) => T, initial: T)`

Performs an iterative computation on the stream

TODO: Explain and add example

#### `join(other: IStreamBuilder<T>)`

Joins two keyed streams, the output stream will contain the elements of the two streams combined, with the key of the element from the left stream.

This is an inner join, so only elements with matching keys will be included in the output.

```typescript
const input = graph.newInput<[key: string, value: number]>()
const other = graph.newInput<[key: string, value: string]>()

const output = input.pipe(join(other))
```

If for example you have a comments, and users stream, you can join them to get a list of comments with the user's name.

```typescript
// The two streams are initially keyed by the userId and commentId respectively
const comments = graph.newInput<[commentId: string, comment: Comment]>()
const users = graph.newInput<[userId: string, user: User]>()

// Map the comments to be "keyed" by the user id
const commentsByUser = comments.pipe(
  map(([commentId, comment]) => [comment.userId, comment] as [string, Comment]),
)

// Join the comments with the users
const output = commentsByUser.pipe(
  join(users),
  map(([_, [userId, [comment, user]]]) => {
    // Re-map the comment to be keyed by the comment id
    // and add the user name to the comment
    return [
      comment.id,
      {
        ...comment,
        userName: user.name,
      },
    ]
  }),
)
```

#### `map(f: (data: T) => T)`

Transforms the elements of the stream using a function

```typescript
const output = input.pipe(map((x) => x + 5))
```

#### `output(messageHandler: (message: Message<T>) => void)`

Outputs the messages of the stream

```typescript
input.pipe(
  output((message) => {
    if (message.type === MessageType.DATA) {
      console.log('Data message', message.data)
    } else if (message.type === MessageType.FRONTIER) {
      console.log('Frontier message', message.data)
    }
  }),
)
```

The message is a `Message<T>` object, with the structure:

```typescript
type Message<T> =
  | {
      type: typeof MessageType.DATA
      data: DataMessage<T>
    }
  | {
      type: typeof MessageType.FRONTIER
      data: FrontierMessage
    }
```

A data messages represents a change to the output data, and has the following data payload:

```typescript
type DataMessage<T> = {
  version: Version
  collection: MultiSet<T>
}
```

A frontier message represents a new frontier, and has the following data payload:

```typescript
type FrontierMessage = Version | Antichain
```

#### `pipe(operator: (stream: IStreamBuilder<T>) => IStreamBuilder<T>)`

Pipes the stream through a series of operators

```typescript
// You can specify the input and output types for the pipeline.
// Here we are specifying the input type as number and the output type as
// string.
const composedPipeline = pipe<number, string>(
  map((x) => x + 5),
  filter((x) => x % 2 === 0),
  map((x) => x.toString()),
  debug('output'),
)

const output = input.pipe(
  map((x) => x + 1),
  composedPipeline,
)

// Or as a function

const myPipe = (a: number, b: number) =>
  pipe<number, number>(
    map((x) => x + a),
    filter((x) => x % b === 0),
    debug('output'),
  )

const output = input.pipe(myPipe(5, 2))
```

#### `reduce(f: (values: [T, multiplicity: number][]) => [R, multiplicity: number][])`

Performs a reduce operation on the stream grouped by key.

The function `f` takes an array of values and their multiplicities and returns an array of the result and their multiplicities.

```typescript
// Sum a values by key from the input stream
const output = input.pipe(
  map((data) => [data.somethingToKeyOn, data.aValueToSum]),
  reduce((values) => {
    // `values` is an array of [value, multiplicity] pairs for a specific key
    let sum = 0
    for (const [value, multiplicity] of values) {
      sum += value * multiplicity
    }
    return [[sum, 1]]
  }),
  output((message) => {
    if (message.type === MessageType.DATA) {
      // `message.data` is a MultiSet representing the changes to the output
      // data
      // In this example, the output stream will contain the change to the
      // sum of the values for each key.
      console.log(message.data)
    }
  }),
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

## Examples

There are a number of examples in the [packages/d2ts/examples](./packages/d2ts/examples) directory, covering:

- Basic usage (map and filter)
- Joins between two streams
- Iterative computations
- Modeling "includes" using joins

## Implementation Details

This implementation started out as a TypeScript port of the [Materialize blog post](https://materialize.com/blog/differential-from-scratch/), but has diverged quite a bit, adopting a pipeline api pattern, persistence to SQLite, and a few other changes to make the DX better.

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
- [DBSP](https://arxiv.org/abs/2203.16684) (very similar to Differential Dataflow)
