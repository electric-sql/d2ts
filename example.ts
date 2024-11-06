import {
  DifferenceStreamBuilder,
  GraphBuilder,
} from './src/differential-dataflow'
import { MultiSet } from './src/multiset'
import { Antichain, V } from './src/order'

const main = async () => {
  while (true) {
    // Wait for enter key press before continuing
    console.log('Press enter to continue')
    await new Promise<void>((resolve) => {
      process.stdin.once('data', () => {
        resolve()
      })
    })
    run()
  }
}

const run = async () => {
  console.time('one')
  ;(function one() {
    console.log('===')

    const graphBuilder = new GraphBuilder(
      new Antichain([V([0, 0])]),
    )

    const [input_a, writer_a] = graphBuilder.newInput<number>()

    const output = input_a.map((x) => x + 5).filter((x) => x % 2 === 0)
    input_a.negate().concat(output)//.debug('output')
    const graph = graphBuilder.finalize()

    for (let i = 0; i < 10; i++) {
      writer_a.sendData(V([0, i]), new MultiSet([[i, 1]]))
      writer_a.sendFrontier(
        new Antichain([V([i, 0]), V([0, i])]),
      )
      graph.step()
    }
  })()
  console.timeEnd('one')

  console.time('two')
  ;(function two() {
    console.log('===')

    const graphBuilder = new GraphBuilder(
      new Antichain([V([0, 0])]),
    )

    const [input_a, writer_a] = graphBuilder.newInput<[number, number]>()
    const [input_b, writer_b] = graphBuilder.newInput<[number, number]>()

    input_a.join(input_b).count()//.debug('count')
    const graph = graphBuilder.finalize()

    for (let i = 0; i < 2; i++) {
      writer_a.sendData(V([0, i]), new MultiSet([[[1, i], 2]]))
      writer_a.sendData(V([0, i]), new MultiSet([[[2, i], 2]]))

      const a_frontier = new Antichain([
        V([i + 2, 0]),
        V([0, i]),
      ])
      writer_a.sendFrontier(a_frontier)
      writer_b.sendData(V([i, 0]), new MultiSet([[[1, i + 2], 2]]))
      writer_b.sendData(V([i, 0]), new MultiSet([[[2, i + 3], 2]]))
      writer_b.sendFrontier(
        new Antichain([V([i, 0]), V([0, i * 2])]),
      )
      graph.step()
    }

    writer_a.sendFrontier(new Antichain([V([11, 11])]))
    writer_b.sendFrontier(new Antichain([V([11, 11])]))
    graph.step()
  })()
  console.timeEnd('two')

  console.time('three')
  ;(function three() {
    console.log('===')

    const graphBuilder = new GraphBuilder(
      new Antichain([V(0)]),
    )

    const [input_a, writer_a] = graphBuilder.newInput<number>()

    const geometricSeries = (
      stream: DifferenceStreamBuilder<number>,
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

    const output = input_a
      .iterate(geometricSeries)
      .debug('iterate')
      .connectReader()
    const graph = graphBuilder.finalize()

    writer_a.sendData(V(0), new MultiSet([[1, 1]]))
    writer_a.sendFrontier(new Antichain([V(1)]))

    while (output.probeFrontierLessThan(new Antichain([V(1)]))) {
      graph.step()
    }

    writer_a.sendData(
      V(1),
      new MultiSet([
        [16, 1],
        [3, 1],
      ]),
    )
    writer_a.sendFrontier(new Antichain([V(2)]))

    while (output.probeFrontierLessThan(new Antichain([V(2)]))) {
      graph.step()
    }

    writer_a.sendData(V(2), new MultiSet([[3, -1]]))
    writer_a.sendFrontier(new Antichain([V(3)]))

    while (output.probeFrontierLessThan(new Antichain([V(3)]))) {
      graph.step()
    }
  })()
  console.timeEnd('three')
}

main()
