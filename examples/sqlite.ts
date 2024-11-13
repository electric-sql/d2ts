import { GraphBuilder } from '../src/builder'
import { MultiSet } from '../src/multiset'
import { Antichain, v } from '../src/order'
import Database from 'better-sqlite3'

const consolidateExample = (useDb: boolean = false) => {
  const db = useDb ? new Database(':memory:') : undefined

  const graphBuilder = new GraphBuilder(new Antichain([v(0)]), db)

  // Exaple of consolidate operator using SQLite
  const [input, writer] = graphBuilder.newInput<number>()

  // Use consolidate operator to persist state to SQLite
  const output = input.consolidate().debug('output')

  const graph = graphBuilder.finalize()


  console.time('consolidateExample')
  // Send some data
  writer.sendData(
    v(0),
    new MultiSet([
      [1, 1],
      [2, 1],
      [3, 1],
    ]),
  )
  writer.sendFrontier(new Antichain([v(1)]))
  graph.step()

  // Send more data that will be consolidated with previous data
  writer.sendData(
    v(1),
    new MultiSet([
      [2, 1],
      [3, -1],
      [4, 1],
    ]),
  )
  writer.sendFrontier(new Antichain([v(2)]))
  graph.step()

  // Send final data
  writer.sendData(
    v(2),
    new MultiSet([
      [1, -1],
      [5, 1],
    ]),
  )
  writer.sendFrontier(new Antichain([v(3)]))
  graph.step()
  console.timeEnd('consolidateExample')
}

console.log('=== consolidateExample with SQLite ===')
consolidateExample(true)

console.log('=== consolidateExample with SQLite again ===')
consolidateExample(true)

console.log('=== consolidateExample without SQLite ===')
consolidateExample(false)

console.log('=== consolidateExample without SQLite again ===')
consolidateExample(false)
