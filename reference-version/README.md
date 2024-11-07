# This is a reference implementation of Differential Dataflow in Python.

It is taken from https://github.com/ruchirK/python-differential/ and this blog post: https://materialize.com/blog/differential-from-scratch/

## Differential Dataflow, in Python.

WIP. 

This is an implementation of Differential Dataflow in Python, that is meant as a learning
tool. This implementation is not meant to be high performance - for that please go to the
Rust implementation.

Simple explanation of what this code does: users get to define their computations as composition
of functional operators like map/filter/join/count/sum/etc. These computations can even have recursion.
They can then send inputs to those computations and get answers back quickly and efficiently. They can keep
sending new inputs, and changing the inputs in arbitrary ways, and keep getting new answers back quickly
and efficiently, regardless of the computation they defined.

Small terminology note: I started using version instead of time/timestamp, and multiplicity instead of diff, throughout
the code, so I will use those names here as well.

The code includes several preliminary implementations that build on concepts introduced in each other, to make things easier to understand. These preliminary implementations are in the directories `v0` - `v4`. Each directory is entirely self contained, however a lot of the components in a given implementation are duplicated from prior ones. There are 5 preliminary implementations and one final one:
  - `v0`: defines a collection (multiset) of data and implements the various operations (join/reduce/map/etc) over
  a single collection. This is roughly the starting point for "what are we even trying to do?".
  - `v1`: defines a finite, totally ordered, sequence of difference collections, to describe a location that changes. `v1` also
  implements the various operations over such difference collection sequences efficiently. Compared to `v0` the main change in `v1` is
  we need to use indexes to efficiently compute reductions and joins when only a small subset of keys change from one collection
  version to the next.
  - `v2`: extends the approach in `v1` to support an unbounded number of difference collections. Now, we have to explicitly construct a dataflow
  graph, rather than relying on the implicit graph induced by function calls. All of the data travels through dataflow edges (basically queues),
  to operators/nodes (basically a struct that do some `work_function` to effect a computation + hold onto state across invocations of its
  `work_fn`). Each operator still has to output data in order, and binary operators always need to wait for both inputs to become available before they can produce an output. Roughly, this is an approach to implememting something like differential while rejecting a lot of the timely paper.
  - `v3`: extends `v2` to explicitly attach a version (time) label to all messages. Operators also now receive a message when a given version/range of versions will no longer receive any more data. Versions are constrained to be integers.
  - `v4`: extends `v3` to allow versions to be tuples of integers that are totally ordered with the lexicographic order. This implementation is the first that supports `iterate` with changing data, but the user has to specify a cap on the number of iterations.
  TODO: I'm not sure that the cap on the number of iterations is strictly necessary
  - `final`/the toplevel of this directory: extends `v4` to support versions that are partially ordered with the product partial order. The
  versions still have to be tuples of integers.

This implementation is different from other implementations (to the best of my knowledge) in that it doesn't
rely on a scheduler reasoning about the structure of the computation graph and scheduling operators intelligently
to guarantee progress / eventual termination.

Instead, implementation provides the following guarantees:

1. After sending a finite number of collections and advancing the frontiers of all inputs to the dataflow graph past a finite set of
versions, the output should, after a finite number of calls to `graph.step()`, see the correct outputs at those versions and also close
those versions.

2. Eventually, after all inputs have ceased sending new data or advancing frontiers, all nodes in the dataflow graph should stop producing
either new data or new frontier updates iff the dataflow graph does not contain any non-convergent iterative computations.

My understanding is that for acyclic dataflow graphs these properties can be satisfied by:

A. For any set of inputs, all operators are guaranteed to produce their individual expected outputs after a finite number of executions.
So, for example, `reduce` can only produce outputs at versions that are closed, so if no versions are closed, it is to be expected that `reduce`
will not produce any outputs. But once a version is closed, it should produce an output for that version, and potentially others, after a finite
number of executions.

B. All dataflow operators will only ever produce a finite number of output messages (new collections of data / frontier updates) in response
to any one input message (input collections of data / frontier updates).

(I'm not claiming to have proved these properties, and indeed I am not even totally how to.)

For cyclic dataflow graphs, the situation is complicated by the existence of a feedback operator that sends messages in a cycle
to another operator, but with their versions incremented.

```
    def example(collection):
        return (
            collection.map(lambda data: data + 1)
            .map(lambda data: data - 1)
            .negate()
            .concat(collection)
            .consolidate() # This step is mandatory for termination.
        )

    output = input_a.iterate(example).debug("iterate")
    graph = graph_builder.finalize()

    input_a_writer.send_data(Version(0), Collection([(1, 1)]))
    input_a_writer.send_frontier(Antichain([Version(1)]))

    for i in range(0, 10):
        graph.step()
```

Take the following simple example. Here, every step of the iteration takes the
input and applies two consecutive map operators which are collectively a no-op
and the negates the input and concatenates it with itself. Every input therefore
produces the empty collection and this loop should reach fixedpoint in two iterations (two not one because of how `iterate` works and needs to subtract the top-level input on the second iteration).

However, if you remove the `consolidate`, which waits to produce data at a given
version until all inputs have provided all of the data at that version and updated
their frontiers, then there some operator execution orderings for which this loop will continue circulating non-empty differences and never terminate. This is also
a concern in the Rust implementation, and the Rust implementation also requires that all paths from iterative subgraph input to output have a consolidation step
that makes sure all differences at a given version meet up and get cancelled out
(TODO: LINK).

There's a second concern: once fixedpoint has been reached (say at `version(0, 1)` in the example above we know we are done with the computation for the top level `version(0)`.)

We don't then want frontier updates like:

```
Antichain([Version(1, 0), Version(0, 2)])
Antichain([Version(1, 0), Version(0, 3)])
Antichain([Version(1, 0), Version(0, 4)])
...
```
to keep circulating through the iteration subgraph. We'd like instead for one of
the operators to realize "hey, we are done with `Version(0, *)` so we can drop
that from the frontier". This code assigns the feedback operator to this task,
and allows it to drop antichain elements for upper level times that have reached
fixedpoint.

TODO: I want to understand a bit better how timely does this.
TODO: The code for handling this in the feedback operator is not very nice. Ideally, we would be able to express this operation in a more mathematical way. Perhaps capabilities are a more reasonable interface for this?
