import { CollectionArray } from './base-types';

export type KeyedData<T> = [key: string, value: T];

/**
 * A multiset of data.
 */
export class Collection<T> {
  #inner: CollectionArray<T>;

  constructor(data: CollectionArray<T> = []) {
    this.#inner = data;
  }

  /**
   * Apply a function to all records in the collection.
   */
  map<U>(f: (data: T) => U): Collection<U> {
    return new Collection(
      this.#inner.map(([data, multiplicity]) => [f(data), multiplicity])
    );
  }

  /**
   * Filter out records for which a function f(record) evaluates to False.
   */
  filter(f: (data: T) => boolean): Collection<T> {
    return new Collection(
      this.#inner.filter(([data, _]) => f(data))
    );
  }

  /**
   * Negate all multiplicities in the collection.
   */
  negate(): Collection<T> {
    return new Collection(
      this.#inner.map(([data, multiplicity]) => [data, -multiplicity])
    );
  }

  /**
   * Concatenate two collections together.
   */
  concat(other: Collection<T>): Collection<T> {
    const out: CollectionArray<T> = [];
    out.push(...this.#inner);
    out.push(...other.#inner);
    return new Collection(out);
  }

  /**
   * Produce as output a collection that is logically equivalent to the input
   * but which combines identical instances of the same record into one
   * (record, multiplicity) pair.
   */
  consolidate(): Collection<T> {
    const consolidated = new Map<string, number>();
    
    for (const [data, multiplicity] of this.#inner) {
      const key = JSON.stringify(data);
      const currentCount = consolidated.get(key) || 0;
      consolidated.set(key, currentCount + multiplicity);
    }

    const result: CollectionArray<T> = [];
    for (const [key, multiplicity] of consolidated.entries()) {
      if (multiplicity !== 0) {
        result.push([JSON.parse(key) as T, multiplicity]);
      }
    }

    result.sort((a, b) => JSON.stringify(a[0]).localeCompare(JSON.stringify(b[0])));
    return new Collection(result);
  }

  /**
   * Match pairs (k, v1) and (k, v2) from the two input collections and produce (k, (v1, v2)).
   */
  join<U>(other: Collection<KeyedData<U>>): Collection<KeyedData<[T, U]>> {
    // TODO: validate that both are keyed collections
    const out: CollectionArray<KeyedData<[T, U]>> = [];
    
    for (const [[k1, v1], d1] of this.#inner as CollectionArray<KeyedData<T>>) {
      for (const [[k2, v2], d2] of other.#inner) {
        if (k1 === k2) {
          out.push([[k1, [v1, v2]], d1 * d2]);
        }
      }
    }
    
    return new Collection(out);
  }

  /**
   * Apply a reduction function to all record values, grouped by key.
   */
  reduce<U = T>(f: (vals: [T, number][]) => [U, number][]): Collection<U> {
    // TODO: validate that the collection is keyed
    const keys = new Map<string, [T, number][]>();
    const out: CollectionArray<U> = [];

    for (const [[key, val], multiplicity] of this.#inner as CollectionArray<KeyedData<T>>) {
      const keyStr = JSON.stringify(key);
      const existing = keys.get(keyStr) || [];
      existing.push([val, multiplicity]);
      keys.set(keyStr, existing);
    }

    for (const [key, vals] of keys.entries()) {
      const results = f(vals);
      for (const [val, multiplicity] of results) {
        out.push([[key, val], multiplicity] as [U, number]);
      }
    }

    return new Collection(out);
  }

  /**
   * Count the number of times each key occurs in the collection.
   */
  count(): Collection<number> {
    return this.reduce<number>((vals: [T, number][]): [number, number][] => {
      let out = 0;
      for (const [_, multiplicity] of vals) {
        out += multiplicity;
      }
      return [[out, 1]];
    });
  }

  /**
   * Produce the sum of all the values paired with a key, for all keys in the collection.
   */
  sum(): Collection<number> {
    return this.reduce<number>((vals: [T, number][]): [number, number][] => {
      let out = 0;
      for (const [val, multiplicity] of vals) {
        out += (val as unknown as number) * multiplicity;
      }
      return [[out, 1]];
    });
  }

  /**
   * Produce the minimum value associated with each key in the collection.
   * 
   * Note that no record may have negative multiplicity when computing the min,
   * as it is unclear what exactly the minimum record is in that case.
   */
  min(): Collection<T> {
    return this.reduce((vals: [T, number][]): [T, number][] => {
      const consolidated = new Map<string, [T, number]>();
      
      for (const [val, multiplicity] of vals) {
        const key = JSON.stringify(val);
        const current = consolidated.get(key)?.[1] || 0;
        consolidated.set(key, [val, current + multiplicity]);
      }

      const validVals = Array.from(consolidated.values())
        .filter(([_, multiplicity]) => multiplicity !== 0);

      if (validVals.length === 0) return [];

      let minEntry = validVals[0];
      for (const entry of validVals) {
        if (entry[1] <= 0) {
          throw new Error('Negative multiplicities not allowed in min operation');
        }
        if (JSON.stringify(entry[0]) < JSON.stringify(minEntry[0])) {
          minEntry = entry;
        }
      }

      return [[minEntry[0], 1]];
    });
  }

  /**
   * Produce the maximum value associated with each key in the collection.
   * 
   * Note that no record may have negative multiplicity when computing the max,
   * as it is unclear what exactly the maximum record is in that case.
   */
  max(): Collection<T> {
    return this.reduce((vals: [T, number][]): [T, number][] => {
      const consolidated = new Map<string, [T, number]>();
      
      for (const [val, multiplicity] of vals) {
        const key = JSON.stringify(val);
        const current = consolidated.get(key)?.[1] || 0;
        consolidated.set(key, [val, current + multiplicity]);
      }

      const validVals = Array.from(consolidated.values())
        .filter(([_, multiplicity]) => multiplicity !== 0);

      if (validVals.length === 0) return [];

      let maxEntry = validVals[0];
      for (const entry of validVals) {
        if (entry[1] <= 0) {
          throw new Error('Negative multiplicities not allowed in max operation');
        }
        if (JSON.stringify(entry[0]) > JSON.stringify(maxEntry[0])) {
          maxEntry = entry;
        }
      }

      return [[maxEntry[0], 1]];
    });
  }

  /**
   * Reduce the collection to a set of elements (from a multiset).
   * 
   * Note that no record may have negative multiplicity when producing this set,
   * as elements of sets may only have multiplicity one, and it is unclear that is
   * an appropriate output for elements with negative multiplicity.
   */
  distinct(): Collection<T> {
    return this.reduce((vals: [T, number][]): [T, number][] => {
      const consolidated = new Map<string, [T, number]>();
      
      for (const [val, multiplicity] of vals) {
        const key = JSON.stringify(val);
        const current = consolidated.get(key)?.[1] || 0;
        consolidated.set(key, [val, current + multiplicity]);
      }

      const validVals = Array.from(consolidated.values())
        .filter(([_, multiplicity]) => multiplicity !== 0);

      for (const [_, multiplicity] of validVals) {
        if (multiplicity <= 0) {
          throw new Error('Negative multiplicities not allowed in distinct operation');
        }
      }

      return validVals.map(([val, _]) => [val, 1]);
    });
  }

  /**
   * Repeatedly invoke a function f on a collection, and return the result
   * of applying the function an infinite number of times (fixedpoint).
   * 
   * Note that if the function does not converge to a fixedpoint this implementation
   * will run forever.
   */
  iterate(f: (collection: Collection<T>) => Collection<T>): Collection<T> {
    let curr = new Collection(this.#inner);
    
    while (true) {
      const result = f(curr);
      if (JSON.stringify(result.#inner) === JSON.stringify(curr.#inner)) {
        break;
      }
      curr = result;
    }
    
    return curr;
  }

  extend(other: CollectionArray<T>): void {
    this.#inner.push(...other);
  }

  getInner(): CollectionArray<T> {
    return this.#inner;
  }
} 