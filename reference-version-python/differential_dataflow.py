"""An implementation of differential dataflow.

Compared to the Rust implementation, this implementation is both much less performant
and more restrictive. Specifically, multiplicities in collections are constrained to
be integers, and versions (timestamps in the Rust codebase) are constrained to be
Version objects (integer tuples ordered by the product partial order).
"""

from collections import defaultdict

from collection import Collection
from graph import (
    BinaryOperator,
    DifferenceStreamReader,
    DifferenceStreamWriter,
    Graph,
    MessageType,
    UnaryOperator,
)
from index import Index
from order import Version, Antichain

from pprint import pprint


class DifferenceStreamBuilder:
    """A representation of a dataflow edge as the dataflow graph is being built.

    This object is only used to set up the dataflow graph, and does not actually
    interact with any data. Manually creating an instance of this object is highly
    unexpected - instead more normal usage would be to create an instance using
    the new_input method on GraphBuilder.
    """

    def __init__(self, graph):
        self._writer = DifferenceStreamWriter()
        self.graph = graph

    def connect_reader(self):
        return self._writer._new_reader()

    def writer(self):
        return self._writer

    def map(self, f):
        output = DifferenceStreamBuilder(self.graph)
        operator = MapOperator(
            self.connect_reader(), output.writer(), f, self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def filter(self, f):
        output = DifferenceStreamBuilder(self.graph)
        operator = FilterOperator(
            self.connect_reader(), output.writer(), f, self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def negate(self):
        output = DifferenceStreamBuilder(self.graph)
        operator = NegateOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def concat(self, other):
        assert id(self.graph) == id(other.graph)
        output = DifferenceStreamBuilder(self.graph)
        operator = ConcatOperator(
            self.connect_reader(),
            other.connect_reader(),
            output.writer(),
            self.graph.frontier(),
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def debug(self, name=""):
        output = DifferenceStreamBuilder(self.graph)
        operator = DebugOperator(
            self.connect_reader(), output.writer(), name, self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def join(self, other):
        assert id(self.graph) == id(other.graph)
        output = DifferenceStreamBuilder(self.graph)
        operator = JoinOperator(
            self.connect_reader(),
            other.connect_reader(),
            output.writer(),
            self.graph.frontier(),
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def count(self):
        output = DifferenceStreamBuilder(self.graph)
        operator = CountOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def consolidate(self):
        output = DifferenceStreamBuilder(self.graph)
        operator = ConsolidateOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def distinct(self):
        output = DifferenceStreamBuilder(self.graph)
        operator = DistinctOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def _start_scope(self):
        new_frontier = self.graph.frontier().extend()
        self.graph.push_frontier(new_frontier)

    def _end_scope(self):
        self.graph.pop_frontier()

    def _ingress(self):
        output = DifferenceStreamBuilder(self.graph)
        operator = IngressOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def _egress(self):
        output = DifferenceStreamBuilder(self.graph)
        operator = EgressOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def iterate(self, f):
        self._start_scope()
        feedback_stream = DifferenceStreamBuilder(self.graph)
        entered = self._ingress().concat(feedback_stream)
        result = f(entered)
        feedback_operator = FeedbackOperator(
            result.connect_reader(), 1, feedback_stream.writer(), self.graph.frontier()
        )
        self.graph.add_stream(feedback_stream)
        self.graph.add_operator(feedback_operator)
        self._end_scope()
        return result._egress()


class GraphBuilder:
    """A representation of a dataflow graph as it is being built."""

    def __init__(self, initial_frontier):
        self.streams = []
        self.operators = []
        self.frontier_stack = [initial_frontier]

    def new_input(self):
        stream_builder = DifferenceStreamBuilder(self)
        self.streams.append(stream_builder.connect_reader())
        return stream_builder, stream_builder.writer()

    def add_operator(self, operator):
        self.operators.append(operator)

    def add_stream(self, stream):
        self.streams.append(stream)

    def frontier(self):
        return self.frontier_stack[-1]

    def push_frontier(self, new_frontier):
        self.frontier_stack.append(new_frontier)

    def pop_frontier(self):
        self.frontier_stack.pop()

    def finalize(self):
        return Graph(self.streams, self.operators)


class LinearUnaryOperator(UnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    self.output.send_data(version, f(collection))
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_frontier().less_equal(frontier)
                    self.set_input_frontier(frontier)

            assert self.output_frontier.less_equal(self.input_frontier())
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


class MapOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def map_inner(collection):
            return collection.map(f)

        super().__init__(input_a, output, map_inner, initial_frontier)


class FilterOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def filter_inner(collection):
            return collection.filter(f)

        super().__init__(input_a, output, filter_inner, initial_frontier)


class NegateOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        def negate_inner(collection):
            return collection.negate()

        super().__init__(input_a, output, negate_inner, initial_frontier)


class ConcatOperator(BinaryOperator):
    def __init__(self, input_a, input_b, output, initial_frontier):
        def inner():
            for (typ, msg) in self.input_a_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_a_frontier().less_equal(frontier)
                    self.set_input_a_frontier(frontier)
            for (typ, msg) in self.input_b_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_b_frontier().less_equal(frontier)
                    self.set_input_b_frontier(frontier)

            input_frontier = self.input_a_frontier().meet(self.input_b_frontier())
            assert self.output_frontier.less_equal(input_frontier)
            if self.output_frontier.less_than(input_frontier):
                self.output_frontier = input_frontier
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, input_b, output, inner, initial_frontier)


class ConsolidateOperator(UnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        self.collections = defaultdict(Collection)

        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    self.collections[version]._extend(collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_frontier().less_equal(frontier)
                    self.set_input_frontier(frontier)
            finished_versions = [
                version
                for version in self.collections.keys()
                if self.input_frontier().less_equal_version(version) is not True
            ]
            for version in finished_versions:
                collection = self.collections.pop(version).consolidate()
                self.output.send_data(version, collection)
            assert self.output_frontier.less_equal(self.input_frontier())
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


class DebugOperator(UnaryOperator):
    def __init__(self, input_a, output, name, initial_frontier):
        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    print(
                        f"debug {name} data: version: {version} collection: {collection}"
                    )
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_frontier().less_equal(frontier)
                    self.set_input_frontier(frontier)
                    print(f"debug {name} notification: frontier {frontier}")
                    assert self.output_frontier.less_equal(self.input_frontier())
                    if self.output_frontier.less_than(self.input_frontier()):
                        self.output_frontier = self.input_frontier()
                        self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


class JoinOperator(BinaryOperator):
    def __init__(self, input_a, input_b, output, initial_frontier):
        self.index_a = Index()
        self.index_b = Index()

        def inner():
            delta_a = Index()
            delta_b = Index()
            for (typ, msg) in self.input_a_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    for ((key, value), multiplicity) in collection._inner:
                        delta_a.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_a_frontier().less_equal(frontier)
                    self.set_input_a_frontier(frontier)
            for (typ, msg) in self.input_b_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    for ((key, value), multiplicity) in collection._inner:
                        delta_b.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_b_frontier().less_equal(frontier)
                    self.set_input_b_frontier(frontier)

            results = defaultdict(Collection)
            for (version, collection) in delta_a.join(self.index_b):
                results[version]._extend(collection)

            self.index_a.append(delta_a)

            for (version, collection) in self.index_a.join(delta_b):
                results[version]._extend(collection)

            for (version, collection) in results.items():
                self.output.send_data(version, collection)
            self.index_b.append(delta_b)

            input_frontier = self.input_a_frontier().meet(self.input_b_frontier())
            assert self.output_frontier.less_equal(input_frontier)
            if self.output_frontier.less_than(input_frontier):
                self.output_frontier = input_frontier
                self.output.send_frontier(self.output_frontier)
                self.index_a.compact(self.output_frontier)
                self.index_b.compact(self.output_frontier)

        super().__init__(input_a, input_b, output, inner, initial_frontier)


class ReduceOperator(UnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        self.index = Index()
        self.index_out = Index()
        self.keys_todo = defaultdict(set)

        def subtract_values(first, second):
            result = defaultdict(int)
            for (v1, m1) in first:
                result[v1] += m1
            for (v2, m2) in second:
                result[v2] -= m2

            return [
                (val, multiplicity)
                for (val, multiplicity) in result.items()
                if multiplicity != 0
            ]

        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    for ((key, value), multiplicity) in collection._inner:
                        self.index.add_value(key, version, (value, multiplicity))
                        self.keys_todo[version].add(key)
                        for v2 in self.index.versions(key):
                            self.keys_todo[version.join(v2)].add(key)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_frontier().less_equal(frontier)
                    self.set_input_frontier(frontier)

            finished_versions = [
                version
                for version in self.keys_todo.keys()
                if self.input_frontier().less_equal_version(version) is not True
            ]

            finished_versions.sort()
            for version in finished_versions:
                keys = self.keys_todo.pop(version)
                result = []
                for key in keys:
                    curr = self.index.reconstruct_at(key, version)
                    curr_out = self.index_out.reconstruct_at(key, version)
                    out = f(curr)
                    delta = subtract_values(out, curr_out)
                    for (value, multiplicity) in delta:
                        result.append(((key, value), multiplicity))
                        self.index_out.add_value(key, version, (value, multiplicity))
                if result != []:
                    self.output.send_data(version, Collection(result))

            assert self.output_frontier.less_equal(self.input_frontier())
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)
                self.index.compact(self.output_frontier)
                self.index_out.compact(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


class CountOperator(ReduceOperator):
    def __init__(self, input_a, output, initial_frontier):
        def count_inner(vals):
            out = 0
            for (_, diff) in vals:
                out += diff
            return [(out, 1)]

        super().__init__(input_a, output, count_inner, initial_frontier)


class DistinctOperator(ReduceOperator):
    def __init__(self, input_a, output, initial_frontier):
        def distinct_inner(vals):
            consolidated = defaultdict(int)
            for (val, diff) in vals:
                consolidated[val] += diff
            for (val, diff) in consolidated.items():
                assert diff >= 0
            return [(val, 1) for (val, diff) in consolidated.items() if diff > 0]

        super().__init__(input_a, output, distinct_inner, initial_frontier)


class FeedbackOperator(UnaryOperator):
    def __init__(self, input_a, step, output, initial_frontier):
        # Map from top-level version -> set of messages where we have
        # sent some data at that version
        self.in_flight_data = defaultdict(set)
        # Versions where a given top-level version has updated
        # its iteration without sending any data.
        self.empty_versions = defaultdict(set)

        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    new_version = version.apply_step(step)
                    truncated = new_version.truncate()
                    self.output.send_data(new_version, collection)

                    # Record that we sent data at this version.
                    self.in_flight_data[truncated].add(new_version)
                    # Make sure we track that we are iterating at this top-level
                    # version if we haven't already
                    if truncated not in self.empty_versions:
                        self.empty_versions[truncated] = set()
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_frontier().less_equal(frontier)
                    self.set_input_frontier(frontier)

            # Increment the current input frontier
            incremented_input_frontier = self.input_frontier().apply_step(step)
            # Grab all of the elements from the potential output frontier.
            elements = incremented_input_frontier._elements()
            # Partition every element from this potential output frontier into one of
            # two sets, either elements to keep, or elements to reject.
            candidate_output_frontier = []
            rejected = []
            for elem in elements:
                truncated = elem.truncate()

                # Always keep a frontier element if there is are differences associated
                # with its top-level version that are still in flight.
                if len(self.in_flight_data[truncated]) != 0:
                    candidate_output_frontier.append(elem)

                    # We can stop remembering any versions that will be closed
                    # by this frontier element.
                    closed = {
                        x for x in self.in_flight_data[truncated] if x.less_than(elem)
                    }
                    self.in_flight_data[truncated] -= closed
                else:
                    # This frontier element does not have any differences associated with its
                    # top-level version that were not closed out by prior frontier updates.

                    # Remember that we observed an "empty" update for this top-level version.
                    self.empty_versions[truncated].add(elem)

                    # Don't do anything if we haven't observed at least three "empty" frontier
                    # updates for this top-level time.
                    if len(self.empty_versions[truncated]) <= 3:
                        candidate_output_frontier.append(elem)
                    else:
                        self.in_flight_data.pop(truncated)
                        self.empty_versions.pop(truncated)
                        rejected.append(elem)

            # Ensure that we can still send data at all other top-level
            # versions that were not rejected.
            for r in rejected:
                for truncated in self.in_flight_data.keys():
                    candidate_output_frontier.append(r.join(truncated.extend()))

            # Construct a minimal antichain from the set of candidate elements.
            candidate_output_frontier = Antichain(candidate_output_frontier)

            assert self.output_frontier.less_equal(candidate_output_frontier)
            if self.output_frontier.less_than(candidate_output_frontier):
                self.output_frontier = candidate_output_frontier
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)

    def connect_loop(output):
        self.output = output


class IngressOperator(UnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    new_version = version.extend()
                    self.output.send_data(new_version, collection)
                    self.output.send_data(
                        new_version.apply_step(1), collection.negate()
                    )
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    new_frontier = frontier.extend()
                    assert self.input_frontier().less_equal(new_frontier)
                    self.set_input_frontier(new_frontier)

            assert self.output_frontier.less_equal(self.input_frontier())
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


class EgressOperator(UnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    new_version = version.truncate()
                    self.output.send_data(new_version, collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    new_frontier = frontier.truncate()
                    assert self.input_frontier().less_equal(new_frontier)
                    self.set_input_frontier(new_frontier)

            assert self.output_frontier.less_equal(self.input_frontier())
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


if __name__ == "__main__":
    print('===')
    graph_builder = GraphBuilder(Antichain([Version([0, 0])]))
    input_a, input_a_writer = graph_builder.new_input()
    output = input_a.map(lambda data: data + 5).filter(lambda data: data % 2 == 0)
    input_a.negate().concat(output).debug("output")
    graph = graph_builder.finalize()

    for i in range(0, 10):
        input_a_writer.send_data(Version([0, i]), Collection([(i, 1)]))
        input_a_writer.send_frontier(Antichain([Version([i, 0]), Version([0, i])]))
        graph.step()

    print('===')
    graph_builder = GraphBuilder(Antichain([Version([0, 0])]))
    input_a, input_a_writer = graph_builder.new_input()
    input_b, input_b_writer = graph_builder.new_input()

    input_a.join(input_b).count().debug("count")
    graph = graph_builder.finalize()

    for i in range(0, 2):
        input_a_writer.send_data(Version([0, i]), Collection([((1, i), 2)]))
        input_a_writer.send_data(Version([0, i]), Collection([((2, i), 2)]))

        a_frontier = Antichain([Version([i + 2, 0]), Version([0, i])])
        input_a_writer.send_frontier(a_frontier)
        input_b_writer.send_data(Version([i, 0]), Collection([((1, i + 2), 2)]))
        input_b_writer.send_data(Version([i, 0]), Collection([((2, i + 3), 2)]))
        input_b_writer.send_frontier(Antichain([Version([i, 0]), Version([0, i * 2])]))
        graph.step()

    input_a_writer.send_frontier(Antichain([Version([11, 11])]))
    input_b_writer.send_frontier(Antichain([Version([11, 11])]))
    graph.step()

    print('===')

    graph_builder = GraphBuilder(Antichain([Version(0)]))
    input_a, input_a_writer = graph_builder.new_input()

    def geometric_series(collection):
        return (
            collection.map(lambda data: data * 2)
            .concat(collection)
            .filter(lambda data: data <= 50)
            .map(lambda data: (data, ()))
            .distinct()
            .map(lambda data: data[0])
            # .debug("map")
            .consolidate()
            # .debug("consolidate")
        )

    output = input_a.iterate(geometric_series).debug("iterate").connect_reader()
    graph = graph_builder.finalize()

    input_a_writer.send_data(Version(0), Collection([(1, 1)]))
    input_a_writer.send_frontier(Antichain([Version(1)]))

    while output.probe_frontier_less_than(Antichain([Version(1)])):
        graph.step()

    input_a_writer.send_data(Version(1), Collection([(16, 1), (3, 1)]))
    input_a_writer.send_frontier(Antichain([Version(2)]))

    while output.probe_frontier_less_than(Antichain([Version(2)])):
        graph.step()

    input_a_writer.send_data(Version(2), Collection([(3, -1)]))
    input_a_writer.send_frontier(Antichain([Version(3)]))

    while output.probe_frontier_less_than(Antichain([Version(3)])):
        graph.step()
