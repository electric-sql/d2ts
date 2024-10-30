import { describe, it, expect } from 'vitest';
import { GraphBuilder } from '../src/differential-dataflow';
import { Version, Antichain } from '../src/order';
import { Collection } from '../src/collection';

describe('DifferentialDataflow', () => {
  it('should handle basic map and filter operations', () => {
    const graphBuilder = new GraphBuilder(new Antichain([new Version([0, 0])]));
    const [input, inputWriter] = graphBuilder.newInput<number>();
    
    // Create a pipeline that adds 5 to each number and filters even numbers
    const output = input
      .map(data => data + 5)
      .filter(data => data % 2 === 0);
    
    // Add a debug operator to see the results
    input.negate().concat(output).debug("output");
    const graph = graphBuilder.finalize();

    // Send data and check results
    for (let i = 0; i < 10; i++) {
      inputWriter.sendData(
        new Version([0, i]), 
        new Collection([[i, 1]])
      );
      inputWriter.sendFrontier(new Antichain([
        new Version([i, 0]), 
        new Version([0, i])
      ]));
      graph.step();
    }
  });

  it('should handle join and count operations', () => {
    const graphBuilder = new GraphBuilder(new Antichain([new Version([0, 0])]));
    const [inputA, inputAWriter] = graphBuilder.newInput<[string, number]>();
    const [inputB, inputBWriter] = graphBuilder.newInput<[string, number]>();

    inputA.join(inputB).count().debug("count");
    const graph = graphBuilder.finalize();

    for (let i = 0; i < 2; i++) {
      inputAWriter.sendData(
        new Version([0, i]), 
        new Collection([
          [[1, i], 2],
          [[2, i], 2]
        ])
      );

      const aFrontier = new Antichain([
        new Version([i + 2, 0]), 
        new Version([0, i])
      ]);
      inputAWriter.sendFrontier(aFrontier);

      inputBWriter.sendData(
        new Version([i, 0]), 
        new Collection([
          [[1, i + 2], 2],
          [[2, i + 3], 2]
        ])
      );
      inputBWriter.sendFrontier(new Antichain([
        new Version([i, 0]), 
        new Version([0, i * 2])
      ]));
      graph.step();
    }

    inputAWriter.sendFrontier(new Antichain([new Version([11, 11])]));
    inputBWriter.sendFrontier(new Antichain([new Version([11, 11])]));
    graph.step();
  });

  it('should handle iteration', () => {
    const graphBuilder = new GraphBuilder(new Antichain([new Version(0)]));
    const [input, inputWriter] = graphBuilder.newInput<number>();

    const geometricSeries = (collection: Collection<number>) => {
      return collection
        .map(data => data * 2)
        .concat(collection)
        .filter(data => data <= 50)
        .map(data => [data, []] as [number, never[]])
        .distinct()
        .map(data => data[0])
        .consolidate();
    };

    const output = input.iterate(stream => stream.map(geometricSeries));
    const reader = output.debug("iterate").connectReader();
    const graph = graphBuilder.finalize();

    inputWriter.sendData(new Version(0), new Collection([[1, 1]]));
    inputWriter.sendFrontier(new Antichain([new Version(1)]));

    while (reader.probeFrontierLessThan(new Antichain([new Version(1)]))) {
      graph.step();
    }

    inputWriter.sendData(new Version(1), new Collection([[16, 1], [3, 1]]));
    inputWriter.sendFrontier(new Antichain([new Version(2)]));

    while (reader.probeFrontierLessThan(new Antichain([new Version(2)]))) {
      graph.step();
    }

    inputWriter.sendData(new Version(2), new Collection([[3, -1]]));
    inputWriter.sendFrontier(new Antichain([new Version(3)]));

    while (reader.probeFrontierLessThan(new Antichain([new Version(3)]))) {
      graph.step();
    }
  });
}); 