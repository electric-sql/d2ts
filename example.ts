import { GraphBuilder } from './src/differential-dataflow'
import { MultiSet } from './src/multiset'
import { Antichain, Version } from './src/order'


{
  console.log('===')

  const graphBuilder = new GraphBuilder<number>(new Antichain([new Version([0, 0])]))

  const [input_a, writer_a] = graphBuilder.newInput()

  const output = input_a.map(x => x + 5).filter(x => x % 2 === 0)
  input_a.negate().concat(output).debug('output')
  const graph = graphBuilder.finalize()

  for (let i = 0; i < 10; i++) {
    writer_a.sendData(new Version([0, i]), new MultiSet([[i, 1]]))
    writer_a.sendFrontier(new Antichain([new Version([i, 0]), new Version([0, i])]))
    graph.step()
  }
}

{
  console.log('===')

  const graphBuilder = new GraphBuilder<number>(new Antichain([new Version([0, 0])]))

  const [input_a, writer_a] = graphBuilder.newInput()
  const [input_b, writer_b] = graphBuilder.newInput()

  input_a.join(input_b)//.count().debug('count')
  const graph = graphBuilder.finalize()

}
