<p align="center">
  <a href="https://electric-sql.com" target="_blank">
    <picture>
      <source media="(prefers-color-scheme: dark)"
          srcset="https://raw.githubusercontent.com/electric-sql/meta/main/identity/ElectricSQL-logo-next.svg"
      />
      <source media="(prefers-color-scheme: light)"
          srcset="https://raw.githubusercontent.com/electric-sql/meta/main/identity/ElectricSQL-logo-black.svg"
      />
      <img alt="ElectricSQL logo"
          src="https://raw.githubusercontent.com/electric-sql/meta/main/identity/ElectricSQL-logo-black.svg"
      />
    </picture>
  </a>
</p>

<p align="center">
  <a href="https://github.com/electric-sql/d2ts/actions"><img src="https://github.com/electric-sql/d2ts/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/electric-sql/d2ts/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License - Apache 2.0"></a>
  <a href="https://github.com/electric-sql/d2ts/
  ext/milestones"><img src="https://img.shields.io/badge/status-alpha-orange" alt="Status - Alpha"></a>
  <a href="https://discord.electric-sql.com"><img src="https://img.shields.io/discord/933657521581858818?color=5969EA&label=discord" alt="Chat - Discord"></a>
  <a href="https://x.com/ElectricSQL" target="_blank"><img src="https://img.shields.io/twitter/follow/ElectricSQL.svg?style=social&label=Follow @ElectricSQL"></a>
</p>

# D2TS - Differential Dataflow in TypeScript <!-- omit in toc -->

D2TS is a TypeScript implementation of [differential dataflow](https://github.com/MaterializeInc/differential) - a powerful data-parallel programming framework that enables incremental computations over changing input data.

You can use D2TS to build data pipelines that can be executed incrementally, meaning you can process data as it comes in, and only recompute the parts that have changed. This could be as simple as remapping data, or as complex as performing a full join combining two datasources where one is a computed aggregate.

D2TS can be used in conjunction with [ElectricSQL](https://electric-sql.com) to build data pipelines on top of [ShapeStreams](https://electric-sql.com/docs/api/clients/typescript#shapestream) that can be executed incrementally.

A D2TS pipeline is also fully type safe, inferring the types at each step of the pipeline, and supports auto-complete in your IDE.

## Table of Contents

- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Examples](#examples)
- [API](#api)
  - [D2 Graph Construction](#d2-graph-construction)
  - [Input Streams](#input-streams)
  - [Versions and Frontiers](#versions-and-frontiers)
  - [MultiSet as a changeset](#multiset-as-a-changeset)
  - [Operators](#operators)
  - [Using SQLite Backend](#using-sqlite-backend)
- [Implementation Details](#implementation-details)
- [References](#references)

## Key Features

- **Incremental Processing**: Efficiently process changes to input data without recomputing everything
- **Rich Operators**: Supports common operations with a pipeline API:
  - [`buffer`](#buffer): Buffer and emit versions when they are complete
  - [`concat`](#concat): Concatenate two streams
  - [`consolidate`](#consolidate): Consolidates the elements in the stream at each version
  - [`count`](#count): Count elements by key
  - [`distinct`](#distinct): Remove duplicates
  - [`filter`](#filter): Filter elements based on predicates
  - [`filterBy`](#filterby): Filter elements of a keyed stream by keys from another stream
  - [`iterate`](#iterate): Perform iterative computations
  - [`join`](#join): Join two streams
  - [`keyBy`](#keyBy): Key a stream by a property
  - [`map`](#map): Transform elements in a stream
  - [`reduce`](#reduce): Aggregate values by key
  - [`rekey`](#rekey): Change the key of a keyed stream
  - [`unkey`](#unkey): Remove keys from a keyed stream
  - [`output`](#output): Output the messages of the stream
  - [`pipe`](#pipe): Build a pipeline of operators enabling reuse of combinations of operators
  - [`topK`](#topk): Limit results to top K elements based on a comparator
  - [`topKWithIndex`](#topkwithindex): Like topK but includes position indices
  - [`topKWithFractionalIndex`](#topkwithfractionalindex): Like topKWithIndex but with stable fractional indices
  - [`groupBy`](#groupby): Group data by key and apply multiple aggregate functions
    - [`sum`](#sum): Sum values in each group
    - [`count`](#count): Count items in each group
    - [`avg`](#avg): Calculate average in each group
    - [`min`](#min): Find minimum value in each group
    - [`max`](#max): Find maximum value in each group
    - [`median`](#median): Calculate median value in each group
    - [`mode`](#mode): Find most frequent value in each group
  - [`orderBy`](#orderby): Order elements by a value extractor function
  - [`orderByWithIndex`](#orderbywithindex): Order elements and include position indices
  - [`orderByWithFractionalIndex`](#orderbywithfractionalindex): Order elements with stable fractional indices
- **SQLite Integration**: Optional SQLite backend for persisting operator state allowing for larger datasets and resumable pipelines
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

### Using with ElectricSQL

D2TS can be used in conjunction with [ElectricSQL](https://electric-sql.com) to build data pipelines on top of [ShapeStreams](https://electric-sql.com/docs/api/clients/typescript#shapestream) that can be executed incrementally.

> [!NOTE]
> Electric support has not yet been merged to main, you can follow the progress [in this PR](https://github.com/electric-sql/d2ts/pull/11).

Here's an example of how to use D2TS with ElectricSQL:

```typescript
import { D2, map, filter, output } from '@electric-sql/d2ts'
import { electricStreamToD2Input } from '@electric-sql/d2ts/electric'
import { ShapeStream } from '@electric-sql/client'

// Create D2 graph
const graph = new D2({ initialFrontier: 0 })

// Create D2 input
const input = graph.newInput<any>()

// Configure the pipeline
input.pipe(
  map(([key, data]) => data.value),
  filter((value) => value > 10),
  // ... any other processing / joining
  output((msg) => doSomething(msg)),
)

// Finalize graph
graph.finalize()

// Create Electric stream (example)
const electricStream = new ShapeStream({
  url: 'http://localhost:3000/v1/shape',
  params: {
    table: 'items',
    replica: 'full', // <-- IMPORTANT!
  },
})

// Connect Electric stream to D2 input
electricStreamToD2Input(electricStream, input)
```

## Examples

There are a number of examples in the [./examples](./examples) directory, covering:

- [Basic usage](./examples/basic.ts) (map and filter)
- ["Fruit processed"](./examples/fruit-processed.ts) (reduce and consolidate)
- [Joins between two streams](./examples/join.ts)
- [Iterative computations](./examples/iterate.ts)
- [Modeling "includes" using joins](./examples/includes.ts)

## API

### D2 graph construction

```typescript
const graph = new D2({ initialFrontier: 0 })
```

The `D2` constructor takes an optional `options` object with the following properties:

- `initialFrontier`: The initial frontier of the graph, defaults to `0`

An instance of a D2 graph is used to build a dataflow graph, and has the following main methods:

- `newInput<T>(): IStreamBuilder<T>`: Create a new input stream
- `finalize(): void`: Finalize the graph, after this point no more operators or inputs can be added
- `run(): void`: Process all pending versions of the dataflow graph

### Input Streams

Input streams are created using the `newInput<T>()` method, and have the following methods:

- `sendData(version: Version | number | number[], data: MultiSet<T>): void`: Send data to the input stream
- `sendFrontier(version: Antichain | Version | number | number[]): void`: Send a frontier to the input stream

### Versions and Frontiers

Versions are used to represent the version of the data, and are a lattice of integers. For most use cases you will only need to provide a single integer version, and all apis that take a version will work with a single integer. More advanced use cases may require the use of the latice to track multidimensional versions.

Frontiers are used to represent the lower bound of the version of the data that may come in future, and are an antichain of versions. Again in most cases you can just use a single integer version to represent the frontier.

#### Version

There is a `Version` class that represents a version, the prefered way to create a version is using the `v` helper function as this ensures that you reuse the same object for the same version making equality checks and comparisons more efficient:

```typescript
const version = v(1)
```

Multidimensional versions are also supported, and are created using the `v` helper function:

```typescript
const version = v([1, 2])
```

In most cases you will only need to use a single integer version to represent the version which can be passed directly to the `sendData` and `sendFrontier` methods:

```typescript
input.sendData(1, new MultiSet([[1, 1]]))
```

#### Antichain (frontier)

An `Antichain` is a set of versions that are disjoint, it is used to represent the frontier of the data. An antichain can be created using the `Antichain` constructor:

```typescript
const frontier = new Antichain([v(1), v([2])])
```

In most cases you will only need to use a single integer version to represent the frontier and can be passed directly to the `sendFrontier` method:

```typescript
input.sendFrontier(1)
```

### MultiSet as a changeset

A `MultiSet` is a map of values to their multiplicity. It is used to represent the changes to a collection.

A MultiSet is created by passing an array of `[value, multiplicity]` pairs. Here we are creating a MultiSet with the values 1, 2, and 3, each with a multiplicity of 1:

```typescript
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

#### buffer

`buffer()`

Buffers the elements of the stream, emitting a version when the buffer is complete.

```typescript
const output = input.pipe(buffer())
```

#### concat

`concat(other: IStreamBuilder<T>)`

Concatenates two input streams - the output stream will contain the elements of both streams.

```typescript
const output = input.pipe(concat(other))
```

#### consolidate

`consolidate()`

Consolidates the elements in the stream at each version, essentially it ensures the output stream is at the latest known _complete_ version.

```typescript
const output = input.pipe(consolidate())
```

#### count

`count()`

Counts the number of elements in the stream by key

```typescript
const output = input.pipe(
  map((data) => [data.somethingToKeyOn, data]),
  count(),
)
```

#### debug

`debug(name: string)`

Logs the messages of the stream to the console, the name is used to identify the stream in the logs.

```typescript
const output = input.pipe(debug('output'))
```

#### distinct

`distinct()`

Removes duplicate values from the stream by key

```typescript
const output = input.pipe(distinct())
```

#### filter

`filter(predicate: (data: T) => boolean)`

Filters the stream based on a predicate

```typescript
const output = input.pipe(filter((x) => x % 2 === 0))
```

#### filterBy

`filterBy(other: IStreamBuilder<KeyValue<K, unknown>>)`

Filters the elements of a keyed stream, by keys of another stream. This allows you to build pipelines where you have multiple outputs that are related, such as streams of issues and comments for a project.

```typescript
// Create two input streams
const issues = graph.newInput<KeyValue<string, Issue>>()
const comments = graph.newInput<KeyValue<string, Comment>>()

// Filter comments to only include those related to specific issues
const commentsForIssues = comments.pipe(
  keyBy((comment) => comment.issue_id),
  filterBy(issues),
  rekey((comment) => comment.id),
  output((message) => {
    // Process only comments that have matching issues
    console.log('Comment for tracked issue:', message)
  }),
)
```

In this example, only comments with keys that exist in the issues stream will be included in the output.

#### iterate

`iterate<T>(f: (stream: IStreamBuilder<T>) => IStreamBuilder<T>)`

Performs iterative computations on a stream by creating a feedback loop. This allows you to repeatedly process data until it reaches a fixed point or meets specific conditions.

The `iterate` operator takes a function that defines the iteration step. Inside this function, you can apply any series of transformations to the stream, and the results will be fed back into the loop for further iterations.

```typescript
// This example repeatedly doubles numbers and includes previous values,
// filtering out any values > 50
const output = input.pipe(
  iterate((stream) =>
    stream.pipe(
      map((x) => x * 2), // Double each value
      concat(stream), // Include original values
      filter((x) => x <= 50), // Keep only values <= 50
      map((x) => [x, []]), // Convert to keyable format
      distinct(), // Remove duplicates
      map((x) => x[0]), // Convert back to simple values
      consolidate(), // Ensure consistent version updates
    ),
  ),
  debug('results'),
)
```

In this example:

1. The `iterate` function creates a feedback loop on the input stream
2. Each value is doubled, then combined with all previous values
3. Values greater than 50 are filtered out
4. The remaining values are deduplicated and consolidated before the next iteration

The iteration will continue until no new values are produced (reaching a fixed point) or until the frontier advances beyond the iteration scope.

Common use cases for the `iterate` operator include:

- Computing transitive closures in graph algorithms
- Propagating values until convergence
- Implementing fixed-point algorithms
- Simulating recursive processes with bounded results

This powerful operator enables complex recursive computations while maintaining the incremental nature of differential dataflow.

#### join

`join(other: IStreamBuilder<T>, joinType?: JoinType)`

Joins two keyed streams based on matching keys. The `joinType` parameter controls how the join behaves:

- `'inner'` (default): Returns only records that have matching keys in both streams
- `'left'`: Returns all records from the left stream, plus matching records from the right (with nulls for non-matches)
- `'right'`: Returns all records from the right stream, plus matching records from the left (with nulls for non-matches)
- `'full'`: Returns all records from both streams, with nulls for non-matches on either side

```typescript
const input = graph.newInput<[key: string, value: number]>()
const other = graph.newInput<[key: string, value: string]>()

// Inner join - only matching keys
const innerJoin = input.pipe(join(other, 'inner'))

// Left join - all records from input, matching from other
const leftJoin = input.pipe(join(other, 'left'))

// Right join - all records from other, matching from input
const rightJoin = input.pipe(join(other, 'right'))

// Full join - all records from both streams
const fullJoin = input.pipe(join(other, 'full'))
```

Additionally there are type-safe variant of the join operator (`leftJoin`, `rightJoin`, `fullJoin`, `antiJoin`), with appropriate nullable types for the different join types:

```typescript
// The two streams are initially keyed by the userId and commentId respectively
const comments = graph.newInput<[commentId: string, comment: Comment]>()
const users = graph.newInput<[userId: string, user: User]>()

// Map the comments to be "keyed" by the user id
const commentsByUser = comments.pipe(
  map(([commentId, comment]) => [comment.userId, comment] as [string, Comment]),
)

// Left join - keeps all comments, even those without matching users
const output = commentsByUser.pipe(
  leftJoin(users),
  map(([userId, [comment, user]]) => {
    // user can be null in a left join if there's no matching user
    return [
      comment.id,
      {
        ...comment,
        userName: user?.name ?? 'Unknown User',
      },
    ]
  }),
)
```

When using SQLite persistence, you can supply the database as an additional parameter:

```typescript
// Using SQLite persistence
const db = new BetterSQLite3Wrapper(sqlite)
const persistedJoin = input.pipe(join(other, 'inner', db))
```

#### map

`map<U>(f: (data: T) => U)`

Transforms the elements of the stream using a function

```typescript
const output = input.pipe(map((x) => x + 5))
```

#### output

`output(messageHandler: (message: Message<T>) => void)`

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

#### pipe

`pipe(operator: (stream: IStreamBuilder<T>) => IStreamBuilder<T>)`

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

#### reduce

`reduce(f: (values: [T, multiplicity: number][]) => [R, multiplicity: number][])`

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

#### Keying Operators

D2TS provides a set of operators for working with keyed streams, which are useful for operations like joins and grouping.

##### `keyBy`

Keys a stream by a property of each element. This is useful for preparing data for joins or grouping operations.

```typescript
const keyedStream = input.pipe(keyBy((item) => item.id))
```

##### `unkey`

Removes the keys from a keyed stream, returning just the values.

```typescript
const unkeyedStream = keyedStream.pipe(unkey())
```

##### `rekey`

Changes the key of a keyed stream to a new key based on a property of the value.

```typescript
const rekeyedStream = keyedStream.pipe(rekey((item) => item.newKey))
```

Example usage with joins:

```typescript
// Transform comments into [issue_id, comment] pairs for joining
const commentsByIssue = inputComments.pipe(rekey((comment) => comment.issue_id))

// Join comments with issues
const issuesWithComments = issuesForProject.pipe(join(commentsByIssue))
```

#### groupBy

`groupBy(keyExtractor: (data: T) => K, aggregates: Record<string, AggregateFunction<T, any>>)`

Groups data by a key and applies multiple aggregate functions to each group. This is a powerful operator that combines functionality similar to SQL's `GROUP BY` with aggregation functions.

The `keyExtractor` function extracts the grouping key from each data item, and the `aggregates` object maps aggregate names to aggregate functions.

```typescript
// Group sales data by category and compute multiple aggregates
const salesAnalytics = salesData.pipe(
  groupBy(
    // Key extractor function - what to group by
    (item) => ({ category: item.category }),
    // Aggregate functions to apply to each group
    {
      total: sum((item) => item.amount),
      count: count(),
      average: avg((item) => item.amount),
      min: min((item) => item.amount),
      max: max((item) => item.amount),
    },
  ),
)
```

The result will be a stream of objects with the grouping key properties and the computed aggregate values:

```typescript
// Example output item
{
  category: "Electronics",
  total: 2500,
  count: 5,
  average: 500,
  min: 200,
  max: 800
}
```

##### Aggregate Functions

D2TS provides several built-in aggregate functions for use with `groupBy`:

###### `sum`

`sum<T>(valueExtractor?: (value: T) => number)`

Computes the sum of values in each group.

```typescript
// Sum the amount field for each category
groupBy((item) => ({ category: item.category }), {
  total: sum((item) => item.amount),
})
```

###### `count`

`count<T>()`

Counts the number of items in each group.

```typescript
// Count items in each category
groupBy((item) => ({ category: item.category }), {
  itemCount: count(),
})
```

###### `avg`

`avg<T>(valueExtractor?: (value: T) => number)`

Computes the average of values in each group.

```typescript
// Calculate average price by category
groupBy((item) => ({ category: item.category }), {
  averagePrice: avg((item) => item.price),
})
```

###### `min`

`min<T>(valueExtractor?: (value: T) => number)`

Finds the minimum value in each group.

```typescript
// Find minimum price by category
groupBy((item) => ({ category: item.category }), {
  lowestPrice: min((item) => item.price),
})
```

###### `max`

`max<T>(valueExtractor?: (value: T) => number)`

Finds the maximum value in each group.

```typescript
// Find maximum price by category
groupBy((item) => ({ category: item.category }), {
  highestPrice: max((item) => item.price),
})
```

###### `median`

`median<T>(valueExtractor?: (value: T) => number)`

Computes the median (middle value) in each group. For groups with an even number of values, returns the average of the two middle values.

```typescript
// Calculate median price by category
groupBy((item) => ({ category: item.category }), {
  medianPrice: median((item) => item.price),
})
```

###### `mode`

`mode<T>(valueExtractor?: (value: T) => number)`

Finds the most frequent value in each group. If multiple values have the same highest frequency, returns the first one encountered.

```typescript
// Find most common price point by category
groupBy((item) => ({ category: item.category }), {
  mostCommonPrice: mode((item) => item.price),
})
```

##### Combining Multiple Aggregates

The power of `groupBy` comes from combining multiple aggregates in a single operation:

```typescript
// Comprehensive sales analysis by region and product category
const salesAnalysis = salesData.pipe(
  groupBy(
    // Group by both region and category
    (sale) => ({
      region: sale.region,
      category: sale.productCategory,
    }),
    // Apply multiple aggregates
    {
      totalRevenue: sum((sale) => sale.amount),
      orderCount: count(),
      averageOrderValue: avg((sale) => sale.amount),
      smallestOrder: min((sale) => sale.amount),
      largestOrder: max((sale) => sale.amount),
      medianOrder: median((sale) => sale.amount),
      mostCommonAmount: mode((sale) => sale.amount),
    },
  ),
)
```

#### orderBy

`orderBy(valueExtractor: (value: V) => T, options?: { comparator?: (a: T, b: T) => number, limit?: number, offset?: number })`

Orders elements in a keyed stream by a value extracted from each element, with optional limit and offset. This operator orders the entire stream, not just within key groups.

```typescript
// Order all products by price
const orderedProducts = products.pipe(
  map((product) => [product.category, product]), // Key by category
  orderBy((product) => product.price, {
    comparator: (a, b) => a - b, // Sort by price ascending
    limit: 10, // Only keep top 10
  }),
)

// To order the entire collection regardless of category
const allProductsByPrice = products.pipe(
  map((product) => [null, product]), // Key all products by null
  orderBy((product) => product.price, {
    comparator: (a, b) => a - b, // Sort by price ascending
  }),
)
```

#### orderByWithIndex

`orderByWithIndex(valueExtractor: (value: V) => T, options?: { comparator?: (a: T, b: T) => number, limit?: number, offset?: number })`

Similar to `orderBy`, but includes the position index of each element in the result. The output format is `[key, [value, index]]` where index is the position (starting from the offset).

```typescript
// Order all products by price and include their overall rank
const rankedProducts = products.pipe(
  map((product) => [product.category, product]),
  orderByWithIndex((product) => product.price, {
    comparator: (a, b) => b - a, // Sort by price descending
    limit: 5,
  }),
  map(([category, [product, position]]) => ({
    category,
    product,
    rank: position + 1, // Convert to 1-based ranking
  })),
)
```

#### orderByWithFractionalIndex

`orderByWithFractionalIndex(valueExtractor: (value: V) => T, options?: { comparator?: (a: T, b: T) => number, limit?: number, offset?: number })`

An advanced version of `orderByWithIndex` that uses fractional indexing to minimize changes when elements move positions. This is particularly useful for UI applications where you want to minimize DOM updates when the order changes.

```typescript
// Order all products with stable fractional indices
const stableOrderedProducts = products.pipe(
  map((product) => [product.category, product]),
  orderByWithFractionalIndex((product) => product.rating, {
    comparator: (a, b) => b - a, // Sort by rating descending
  }),
  map(([category, [product, fractionalIndex]]) => ({
    category,
    product,
    position: fractionalIndex, // Lexicographically sortable string index
  })),
)
```

For more details on how fractional indexing works, see [Implementing Fractional Indexing](https://observablehq.com/@dgreensp/implementing-fractional-indexing) by David Greenspan.

#### topK

`topK(comparator: (a: T, b: T) => number, options?: { limit?: number, offset?: number })`

Limits the number of results based on a comparator, with optional limit and offset. This works on a keyed stream, where elements are sorted within each key group.

```typescript
// Get the top 5 most expensive products in each category
const topProducts = products.pipe(
  map((product) => [product.category, product]), // Key by category
  topK((a, b) => b.price - a.price, { limit: 5 }), // Sort by price descending
)
```

To sort the entire stream, key all elements by the same value (like `null`):

```typescript
// Get the top 10 most expensive products overall
const topOverall = products.pipe(
  map((product) => [null, product]), // Key all products by null
  topK((a, b) => b.price - a.price, { limit: 10 }),
)
```

#### topKWithIndex

`topKWithIndex(comparator: (a: T, b: T) => number, options?: { limit?: number, offset?: number })`

Similar to `topK`, but includes the position index of each element in the result. The output format is `[key, [value, index]]` where index is the position (starting from the offset).

```typescript
// Get top 5 products with their position in each category
const rankedProducts = products.pipe(
  map((product) => [product.category, product]),
  topKWithIndex((a, b) => b.rating - a.rating, { limit: 5 }),
  map(([category, [product, position]]) => ({
    category,
    product,
    rank: position + 1, // Convert to 1-based ranking
  })),
)
```

#### topKWithFractionalIndex

`topKWithFractionalIndex(comparator: (a: T, b: T) => number, options?: { limit?: number, offset?: number })`

An advanced version of `topKWithIndex` that uses fractional indexing to minimize changes when elements move positions. Instead of integer indices, it assigns string-based fractional indices that are lexicographically sortable.

When elements change position, only the indices of the moved elements are updated, not all elements. This is particularly useful for UI applications where you want to minimize DOM updates.

```typescript
// Get top 10 leaderboard entries with stable fractional indices
const leaderboard = scores.pipe(
  map((score) => ['leaderboard', score]),
  topKWithFractionalIndex((a, b) => b.points - a.points, { limit: 10 }),
  map(([_, [score, fractionalIndex]]) => ({
    ...score,
    position: fractionalIndex, // Lexicographically sortable string index
  })),
)
```

For more details on how fractional indexing works, see [Implementing Fractional Indexing](https://observablehq.com/@dgreensp/implementing-fractional-indexing) by David Greenspan.

### Using SQLite Backend

For persistence and larger datasets, a number of operators are provided that persist to SQLite:

- `consolidate()`: Consolidates data into a single version
- `count()`: Counts the number of elements in a collection
- `distinct()`: Removes duplicates from a collection
- `join()`: Joins two collections
- `map()`: Transforms elements
- `reduce()`: Aggregates values by key
- `groupBy()`: Groups data by key and applies multiple aggregate functions
- `topK()`: Limits the number of results per group based on a comparator, and its variants `topKWithIndex` and `topKWithFractionalIndex`
- `orderBy()`: Orders elements by value, and its variants `orderByWithIndex` and `orderByWithFractionalIndex`

Each take a SQLite database as an optional argument, for example:

```typescript
// Using better-sqlite3
const sqlite = new Database('./my_database.db')
const db = new BetterSQLite3Wrapper(sqlite)

const output = input.pipe(consolidate(db))
```

Alternatively, you can use dependency injection to provide the SQLite database once for a pipeline of operators:

```typescript
// Using better-sqlite3
const sqlite = new Database('./my_database.db')
const db = new BetterSQLite3Wrapper(sqlite)

// Using withSQLite for dependency injection
const output = input.pipe(
  withSQLite(db)(
    map((x) => x + 1),
    reduce((vals) => {
      let sum = 0
      for (const [val, diff] of vals) {
        sum += val * diff
      }
      return [[sum, 1]]
    }),
    distinct()
  )
)
```

This approach makes the code cleaner by not having to pass the database to each operator individually. The `withSQLite` function creates a context where the database is automatically available to all SQLite operators within its scope.

You can also mix both approaches as needed:

```typescript
// Explicitly pass database to one operator
const firstPart = input.pipe(reduce(reducer, db))

// Use dependency injection for subsequent operators
firstPart.pipe(
  withSQLite(db)(
    distinct(),
    consolidate()
  )
)
```

The operators will automatically create the necessary tables and indexes to store the state of the operators. It is advised to use the same database for all operators to ensure that the state is stored in a single location.

The `BetterSQLite3Wrapper` is a wrapper around the `better-sqlite3` library that provides a unified interface for the operators. Other SQLite database drivers can be supported by implementing the `SQLiteDb` interface.

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
