from collection import Collection
from order import Version, Antichain
from differential_dataflow import GraphBuilder


def game_of_life(collection):
    maybe_live_cells = collection.map(lambda data: ((data[0] - 1, data[1] - 1), ()))
    maybe_live_cells = maybe_live_cells.concat(
        collection.map(lambda data: ((data[0] - 1, data[1]), ()))
    )

    maybe_live_cells = maybe_live_cells.concat(
        collection.map(lambda data: ((data[0] - 1, data[1] + 1), ()))
    )
    maybe_live_cells = maybe_live_cells.concat(
        collection.map(lambda data: ((data[0], data[1] - 1), ()))
    )
    maybe_live_cells = maybe_live_cells.concat(
        collection.map(lambda data: ((data[0], data[1] + 1), ()))
    )
    maybe_live_cells = maybe_live_cells.concat(
        collection.map(lambda data: ((data[0] + 1, data[1] - 1), ()))
    )
    maybe_live_cells = maybe_live_cells.concat(
        collection.map(lambda data: ((data[0] + 1, data[1]), ()))
    )
    maybe_live_cells = maybe_live_cells.concat(
        collection.map(lambda data: ((data[0] + 1, data[1] + 1), ()))
    )

    maybe_live_cells = maybe_live_cells.count()
    live_with_three_neighbors = maybe_live_cells.filter(lambda data: data[1] == 3).map(
        lambda data: (data[0], ())
    )
    live_with_two_neighbors = (
        maybe_live_cells.filter(lambda data: data[1] == 2)
        .join(collection.map(lambda data: (data, ())))
        .map(lambda data: (data[0], ()))
    )
    live_next_round = (
        live_with_two_neighbors.concat(live_with_three_neighbors)
        .distinct()
        .map(lambda data: data[0])
    )

    return live_next_round


graph_builder = GraphBuilder(Antichain([Version(0)]))
input_a, input_a_writer = graph_builder.new_input()
output = input_a.iterate(game_of_life).debug("iterate").connect_reader()
graph = graph_builder.finalize()

input_a_writer.send_data(
    Version(0), Collection([((2, 2), 1), ((2, 3), 1), ((2, 4), 1), ((3, 2), 1)])
)
input_a_writer.send_frontier(Antichain([Version(1)]))

while output.probe_frontier_less_than(Antichain([Version(1)])):
    graph.step()
