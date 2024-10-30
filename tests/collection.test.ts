import { describe, it, expect } from 'vitest';
import { Collection } from '../src/collection';

describe('Collection', () => {
  it('should handle basic operations', () => {
    const a = new Collection<[string, string | string[]]>([
      [["apple", "$5"], 2],
      [["banana", "$2"], 1]
    ]);
    const b = new Collection<[string, string | string[]]>([
      [["apple", "$3"], 1],
      [["apple", ["granny smith", "$2"]], 1],
      [["kiwi", "$2"], 1]
    ]);

    // Test concat
    const concat = a.concat(b);
    expect(concat.getInner()).toHaveLength(5);

    // Test join
    const joined = a.join(b);
    expect(joined.getInner()).toHaveLength(2);

    // Test filter
    const filtered = a.filter(data => data[0] !== "apple");
    expect(filtered.getInner()).toHaveLength(1);
    expect(filtered.getInner()[0][0]).toEqual(["banana", "$2"]);

    // Test map
    const mapped = a.map(data => [data[1], data[0]]);
    expect(mapped.getInner()[0][0]).toEqual(["$5", "apple"]);
  });

  it('should handle numeric operations', () => {
    const d = new Collection([
      [["apple", 11], 1],
      [["apple", 3], 2],
      [["banana", 2], 3],
      [["coconut", 3], 1]
    ]);

    // Test sum
    const sum = d.sum();
    console.log(sum.getInner());
    const t = sum.getInner()[0][0]
    expect(sum.getInner()[0][0]).toBe(17);

    // Test count
    const count = d.count();
    console.log(count.getInner());
    expect(count.getInner()[0][0]).toBe(7);
  });

  it('should handle min/max operations', () => {
    const c = new Collection([
      [["apple", "$5"], 2],
      [["banana", "$2"], 1],
      [["apple", "$2"], 20]
    ]);

    // Test min
    const min = c.min();
    expect(min.getInner()[0][0][1]).toBe("$2");

    // Test max
    const max = c.max();
    expect(max.getInner()[0][0][1]).toBe("$5");
  });

  it('should handle iteration', () => {
    const e = new Collection([[1, 1]]);

    const addOne = (collection: Collection<number>) => {
      return collection
        .map(data => data + 1)
        .concat(collection)
        .filter(data => data <= 5)
        .map(data => [data, []])
        .distinct()
        .map(data => data[0])
        .consolidate();
    };

    // @ts-ignore
    const result = e.iterate(addOne).map(data => [data, data * data]);
    expect(result.getInner()).toEqual([
      [[1, 1], 1],
      [[2, 4], 1],
      [[3, 9], 1],
      [[4, 16], 1],
      [[5, 25], 1]
    ]);
  });

  it('should handle negative multiplicities correctly', () => {
    const a = new Collection([[1, 1]]);
    const b = new Collection([[1, -1]]);
    const result = a.concat(b).consolidate();
    expect(result.getInner()).toHaveLength(0);
  });

  it('should throw on invalid operations', () => {
    const a = new Collection([[1, -1]]);
    expect(() => a.min()).toThrow();
    expect(() => a.max()).toThrow();
    expect(() => a.distinct()).toThrow();
  });
}); 