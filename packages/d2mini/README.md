# D2Mini is a minimal IVM implementation based on Differential Dataflow but without versions and frontiers.


### Basic Usage

Here's a simple example that demonstrates the core concepts:

```typescript
import { D2, map, filter, debug, MultiSet, v } from '@electric-sql/d2ts'

// Create a new D2 graph
const graph = new D2()

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
// The key thing to understand is that the MultiSet represents a *change* to
// the data, not the data itself. "Inserts" and "Deletes" are represented as
// an element with a multiplicity of 1 or -1 respectively.
input.sendData(
  new MultiSet([
    [1, 1],
    [2, 1],
    [3, 1],
  ]),
)

// Process the data
graph.run()

// Output will show:
// 6 (from 1 + 5)
// 8 (from 3 + 5)
```
