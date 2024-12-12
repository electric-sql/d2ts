import { DifferenceStreamBuilder, GraphBuilder } from '../src/builder'
import { MultiSet } from '../src/multiset'
import { Antichain, v } from '../src/order'

const graphBuilder = new GraphBuilder(new Antichain([v(0)]))

const [input_a, writer_a] = graphBuilder.newInput<number>()

const geometricSeries = (
  stream: DifferenceStreamBuilder<number>
): DifferenceStreamBuilder<number> => {
  return (
    stream
      // .debug('stream')
      .map((x) => x * 2)
      .concat(stream)
      .filter((x) => x <= 50)
      // .debug('filter')
      .map((x) => [x, []])
      // .debug('map1')
      .distinct()
      // .debug('distinct')
      .map((x) => x[0])
      // .debug('map2')
      .consolidate() as DifferenceStreamBuilder<number>
  )
  // .debug('consolidate') as DifferenceStreamBuilder<number>
}

const output = input_a.iterate(geometricSeries).debug('iterate').connectReader()
const graph = graphBuilder.finalize()

writer_a.sendData(v(0), new MultiSet([[1, 1]]))
writer_a.sendFrontier(new Antichain([v(1)]))

while (output.probeFrontierLessThan(new Antichain([v(1)]))) {
  graph.step()
}

writer_a.sendData(
  v(1),
  new MultiSet([
    [16, 1],
    [3, 1],
  ])
)
writer_a.sendFrontier(new Antichain([v(2)]))

while (output.probeFrontierLessThan(new Antichain([v(2)]))) {
  graph.step()
}

writer_a.sendData(v(2), new MultiSet([[3, -1]]))
writer_a.sendFrontier(new Antichain([v(3)]))

while (output.probeFrontierLessThan(new Antichain([v(3)]))) {
  graph.step()
}
