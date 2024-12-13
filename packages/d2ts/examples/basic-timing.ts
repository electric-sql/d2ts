import { D2 } from '../src/index.js'
import { map, filter, negate, concat, join, count } from '../src/operators/index.js'

function one() {
  console.log('=== one: map/filter/negate/concat ===')

  const graph = new D2({ initialFrontier: 0 })
  const input_a = graph.newInput<number>()

  input_a
    .pipe(
      map((x) => x + 5),
      filter((x) => x % 2 === 0),
      concat(input_a.pipe(negate()))
      // debug('output') // uncomment to debug
    )

  graph.finalize()

  for (let i = 0; i < 10; i++) {
    input_a.sendFrontier(i)
    input_a.sendData(i, [[i, 1]])
    graph.run()
  }
}

function two() {
  console.log('=== two: join/count ===')

  const graph = new D2({ initialFrontier: 0 })
  const input_a = graph.newInput<[number, number]>()
  const input_b = graph.newInput<[number, number]>()

  input_a
    .pipe(
      join(input_b),
      count()
      // debug('count') // uncomment to debug
    )

  graph.finalize()

  for (let i = 0; i < 2; i++) {
    input_a.sendData(i + 1, [
      [[1, i], 2],
      [[2, i], 2]
    ])
    input_a.sendFrontier(i + 2)

    input_b.sendData(i, [
      [[1, i + 2], 2],
      [[2, i + 3], 2]
    ])
    input_b.sendFrontier(i)
    graph.run()
  }

  input_a.sendFrontier(11)
  input_b.sendFrontier(11)
  graph.run()
}

const run = async () => {
  console.time('one')
  one()
  console.timeEnd('one')

  console.time('two')
  two()
  console.timeEnd('two')
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
