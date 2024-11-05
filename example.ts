import {
  DifferenceStreamBuilder,
  GraphBuilder,
} from './src/differential-dataflow'
import { MultiSet } from './src/multiset'
import { Antichain, Version } from './src/order'

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

    const graphBuilder = new GraphBuilder<number>(
      new Antichain([new Version([0, 0])]),
    )

    const [input_a, writer_a] = graphBuilder.newInput()

    const output = input_a.map((x) => x + 5).filter((x) => x % 2 === 0)
    input_a.negate().concat(output)//.debug('output')
    const graph = graphBuilder.finalize()

    for (let i = 0; i < 10; i++) {
      writer_a.sendData(new Version([0, i]), new MultiSet([[i, 1]]))
      writer_a.sendFrontier(
        new Antichain([new Version([i, 0]), new Version([0, i])]),
      )
      graph.step()
    }
  })()
  console.timeEnd('one')

  console.time('two')
  ;(function two() {
    console.log('===')

    const graphBuilder = new GraphBuilder<[number, number]>(
      new Antichain([new Version([0, 0])]),
    )

    const [input_a, writer_a] = graphBuilder.newInput()
    const [input_b, writer_b] = graphBuilder.newInput()

    input_a.join(input_b).count()//.debug('count')
    const graph = graphBuilder.finalize()

    for (let i = 0; i < 2; i++) {
      writer_a.sendData(new Version([0, i]), new MultiSet([[[1, i], 2]]))
      writer_a.sendData(new Version([0, i]), new MultiSet([[[2, i], 2]]))

      const a_frontier = new Antichain([
        new Version([i + 2, 0]),
        new Version([0, i]),
      ])
      writer_a.sendFrontier(a_frontier)
      writer_b.sendData(new Version([i, 0]), new MultiSet([[[1, i + 2], 2]]))
      writer_b.sendData(new Version([i, 0]), new MultiSet([[[2, i + 3], 2]]))
      writer_b.sendFrontier(
        new Antichain([new Version([i, 0]), new Version([0, i * 2])]),
      )
      graph.step()
    }

    writer_a.sendFrontier(new Antichain([new Version([11, 11])]))
    writer_b.sendFrontier(new Antichain([new Version([11, 11])]))
    graph.step()
  })()
  console.timeEnd('two')

  console.time('three')
  ;(function three() {
    console.log('===')

    const graphBuilder = new GraphBuilder<number>(
      new Antichain([new Version(0)]),
    )

    const [input_a, writer_a] = graphBuilder.newInput()

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
      // .debug('iterate')
      .connectReader()
    const graph = graphBuilder.finalize()

    writer_a.sendData(new Version(0), new MultiSet([[1, 1]]))
    writer_a.sendFrontier(new Antichain([new Version(1)]))

    while (output.probeFrontierLessThan(new Antichain([new Version(1)]))) {
      graph.step()
    }

    writer_a.sendData(
      new Version(1),
      new MultiSet([
        [16, 1],
        [3, 1],
      ]),
    )
    writer_a.sendFrontier(new Antichain([new Version(2)]))

    while (output.probeFrontierLessThan(new Antichain([new Version(2)]))) {
      graph.step()
    }

    writer_a.sendData(new Version(2), new MultiSet([[3, -1]]))
    writer_a.sendFrontier(new Antichain([new Version(3)]))

    while (output.probeFrontierLessThan(new Antichain([new Version(3)]))) {
      graph.step()
    }
  })()
  console.timeEnd('three')
}

main()
