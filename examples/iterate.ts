import {
  D2,
  Antichain,
  concat,
  consolidate,
  debug,
  distinct,
  filter,
  iterate,
  map,
  MultiSet,
  v,
} from '@electric-sql/d2ts'

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

graph.run()

input.sendData(
  v(1),
  new MultiSet([
    [16, 1],
    [3, 1],
  ]),
)
input.sendFrontier(new Antichain([v(2)]))

graph.run()

input.sendData(v(2), new MultiSet([[3, -1]]))
input.sendFrontier(new Antichain([v(3)]))

graph.run()
