import { D2 } from '../src/index.js'
import { MultiSet } from '../src/multiset.js'
import { Antichain, v } from '../src/order.js'
import {
  iterate,
  debug,
  map,
  consolidate,
  distinct,
  filter,
  concat,
} from '../src/operators.js'

const graph = new D2({ initialFrontier: new Antichain([v(0)]) })

const input = graph.newInput<number>()

const output = input
  .pipe(
    iterate((stream) =>
      stream.pipe(
        map((x) => x * 2),
        concat(stream),
        filter((x) => x <= 50),
        map((x) => [x, []]),
        distinct(),
        map((x) => x[0]),
        consolidate(),
      ),
    ),
    debug('iterate'),
  )
  .connectReader()

graph.finalize()

input.sendData(v(0), new MultiSet([[1, 1]]))
input.sendFrontier(new Antichain([v(1)]))

while (output.probeFrontierLessThan(new Antichain([v(1)]))) {
  graph.step()
}

input.sendData(
  v(1),
  new MultiSet([
    [16, 1],
    [3, 1],
  ]),
)
input.sendFrontier(new Antichain([v(2)]))

while (output.probeFrontierLessThan(new Antichain([v(2)]))) {
  graph.step()
}

input.sendData(v(2), new MultiSet([[3, -1]]))
input.sendFrontier(new Antichain([v(3)]))

while (output.probeFrontierLessThan(new Antichain([v(3)]))) {
  graph.step()
}
