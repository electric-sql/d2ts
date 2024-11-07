import {
  DifferenceStreamBuilder,
  GraphBuilder,
} from './src/builder'
import { MultiSet } from './src/multiset'
import { Antichain, v } from './src/order'

const run = async () => {
  console.time('one')
  ;(function one() {
    console.log('===')

    const graphBuilder = new GraphBuilder(
      new Antichain([v([0, 0])]),
    )

    const [input_a, writer_a] = graphBuilder.newInput<number>()

    const output = input_a.map((x) => x + 5).filter((x) => x % 2 === 0)
    input_a.negate().concat(output)//.debug('output')
    const graph = graphBuilder.finalize()

    for (let i = 0; i < 10; i++) {
      writer_a.sendData(v([0, i]), new MultiSet([[i, 1]]))
      writer_a.sendFrontier(
        new Antichain([v([i, 0]), v([0, i])]),
      )
      graph.step()
    }
  })()
  console.timeEnd('one')

  console.time('two')
  ;(function two() {
    console.log('===')

    const graphBuilder = new GraphBuilder(
      new Antichain([v([0, 0])]),
    )

    const [input_a, writer_a] = graphBuilder.newInput<[number, number]>()
    const [input_b, writer_b] = graphBuilder.newInput<[number, number]>()

    input_a.join(input_b).count()//.debug('count')
    const graph = graphBuilder.finalize()

    for (let i = 0; i < 2; i++) {
      writer_a.sendData(v([0, i]), new MultiSet([[[1, i], 2]]))
      writer_a.sendData(v([0, i]), new MultiSet([[[2, i], 2]]))

      const a_frontier = new Antichain([
        v([i + 2, 0]),
        v([0, i]),
      ])
      writer_a.sendFrontier(a_frontier)
      writer_b.sendData(v([i, 0]), new MultiSet([[[1, i + 2], 2]]))
      writer_b.sendData(v([i, 0]), new MultiSet([[[2, i + 3], 2]]))
      writer_b.sendFrontier(
        new Antichain([v([i, 0]), v([0, i * 2])]),
      )
      graph.step()
    }

    writer_a.sendFrontier(new Antichain([v([11, 11])]))
    writer_b.sendFrontier(new Antichain([v([11, 11])]))
    graph.step()
  })()
  console.timeEnd('two')

  console.time('three')
  ;(function three() {
    console.log('===')

    const graphBuilder = new GraphBuilder(
      new Antichain([v(0)]),
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
      ]),
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
  })()
  console.timeEnd('three')
}

/**
 * After JS is JIT compiled, the first run is slower.
 * use --multi-run to run multiple times (blocked on hitting enter)
 */
const main = async () => {
  const multiRun = process.argv.includes('--multi-run')
  
  if (multiRun) {
    while (true) {
      run()
      // Wait for enter key press before continuing
      console.log('===')
      console.log('Press enter to run again')
      await new Promise<void>((resolve) => {
        process.stdin.once('data', () => {
          resolve()
        })
      })
    }
  } else {
    run()
  }
}

main()
