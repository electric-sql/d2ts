"""The implementation of dataflow graph edge, node, and graph objects, used to run a dataflow program."""

from collections import deque
from enum import Enum

class MessageType(Enum):
    DATA = 1
    FRONTIER = 2


class DifferenceStreamReader:
    """A read handle to a dataflow edge that receives data and frontier updates from a writer.

    The data received over this edge are pairs of (version, Collection) and the frontier
    updates are either integers (in the one dimensional case) or Antichains (in the general
    case).
    """

    def __init__(self, queue):
        self._queue = queue

    def drain(self):
        out = []
        while len(self._queue) > 0:
            out.append(self._queue.pop())
        return out

    def is_empty(self):
        return len(self._queue) == 0

    def probe_frontier_less_than(self, frontier):
        for (typ, msg) in self._queue:
            if typ == MessageType.FRONTIER:
                received_frontier = msg
                if frontier.less_equal(received_frontier):
                    return False
        return True


class DifferenceStreamWriter:
    """A write handle to a dataflow edge that is allowed to publish data and send
    frontier updates.
    """

    def __init__(self):
        self._queues = []
        self.frontier = None

    def send_data(self, version, collection):
        if isinstance(version, int):
            assert self.frontier is None or self.frontier <= version
        else:
            assert self.frontier is None or self.frontier.less_equal_version(version)
        for q in self._queues:
            q.appendleft((MessageType.DATA, (version, collection)))

    def send_frontier(self, frontier):
        if isinstance(frontier, int):
            assert self.frontier is None or self.frontier <= frontier
        else:
            assert self.frontier is None or self.frontier.less_equal(frontier)

        self.frontier = frontier
        for q in self._queues:
            q.appendleft((MessageType.FRONTIER, frontier))

    def _new_reader(self):
        q = deque()
        self._queues.append(q)
        return DifferenceStreamReader(q)


class Operator:
    """A generic implementation of a dataflow operator (node) that has multiple incoming edges (read handles) and
    one outgoing edge (write handle).
    """

    def __init__(self, inputs, output, f, initial_frontier):
        self.inputs = inputs
        self.output = output
        self.f = f
        self.pending_work = False
        self.input_frontiers = [initial_frontier for _ in self.inputs]
        self.output_frontier = initial_frontier

    def run(self):
        self.f()

    def pending_work(self):
        if self.pending_work is True:
            return True
        for input_listener in self.inputs:
            if input_listener.is_empty() is False:
                return True
        return False

    def frontiers(self):
        return (self.input_frontiers, self.output_frontier)


class UnaryOperator(Operator):
    """A convenience implementation of a dataflow operator that has a handle to one
    incoming stream of data, and one handle to an outgoing stream of data.
    """

    def __init__(self, input_a, output, f, initial_frontier):
        super().__init__([input_a], output, f, initial_frontier)

    def input_messages(self):
        return self.inputs[0].drain()

    def input_frontier(self):
        return self.input_frontiers[0]

    def set_input_frontier(self, frontier):
        self.input_frontiers[0] = frontier


class BinaryOperator(Operator):
    """A convenience implementation of a dataflow operator that has a handle to two
    incoming streams of data, and one handle to an outgoing stream of data.
    """

    def __init__(self, input_a, input_b, output, f, initial_frontier):
        super().__init__([input_a, input_b], output, f, initial_frontier)

    def input_a_messages(self):
        return self.inputs[0].drain()

    def input_a_frontier(self):
        return self.input_frontiers[0]

    def set_input_a_frontier(self, frontier):
        self.input_frontiers[0] = frontier

    def input_b_messages(self):
        return self.inputs[1].drain()

    def input_b_frontier(self):
        return self.input_frontiers[1]

    def set_input_b_frontier(self, frontier):
        self.input_frontiers[1] = frontier


class Graph:
    """An implementation of a dataflow graph.

    This implementation needs to keep the entire set of nodes so that they
    may be run, and only keeps a set of read handles to all edges for debugging
    purposes. Calling this a graph instead of a 'bag of nodes' is misleading, because
    this object does not actually know anything about the connections between the
    various nodes.
    """

    def __init__(self, streams, operators):
        self.streams = streams
        self.operators = operators

    def step(self):
        for op in self.operators:
            op.run()
