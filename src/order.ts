/**
 * A partially, or totally ordered version (time), consisting of a tuple of integers.
 * 
 * All versions within a scope of a dataflow must have the same dimension/number
 * of coordinates. One dimensional versions are totally ordered. Multidimensional
 * versions are partially ordered by the product partial order.
 */
export class Version {
  #inner: number[];

  constructor(version: number | number[]) {
    if (typeof version === 'number') {
      this.#validateNumber(version);
      this.#inner = [version];
    } else {
      version.forEach(this.#validateNumber);
      this.#inner = [...version];
    }
  }

  #validateNumber(n: number): void {
    if (n < 0 || !Number.isInteger(n)) {
      throw new Error('Version numbers must be non-negative integers');
    }
  }

  equals(other: Version): boolean {
    return this.#inner.length === other.#inner.length && 
           this.#inner.every((v, i) => v === other.#inner[i]);
  }

  lessThan(other: Version): boolean {
    if (this.lessEqual(other) && !this.equals(other)) {
      return true;
    }
    return false;
  }

  lessEqual(other: Version): boolean {
    this.#validate(other);
    return this.#inner.every((v, i) => v <= other.#inner[i]);
  }

  join(other: Version): Version {
    this.#validate(other);
    const out = this.#inner.map((v, i) => Math.max(v, other.#inner[i]));
    return new Version(out);
  }

  meet(other: Version): Version {
    this.#validate(other);
    const out = this.#inner.map((v, i) => Math.min(v, other.#inner[i]));
    return new Version(out);
  }

  #validate(other: Version): void {
    if (this.#inner.length !== other.#inner.length) {
      throw new Error('Version dimensions must match');
    }
  }

  advanceBy(frontier: Antichain): Version {
    if (frontier.isEmpty()) {
      return this;
    }
    let result = this.join(frontier.elements[0]);
    for (const elem of frontier.elements) {
      result = result.meet(this.join(elem));
    }
    return result;
  }

  extend(): Version {
    return new Version([...this.#inner, 0]);
  }

  truncate(): Version {
    const elements = [...this.#inner];
    elements.pop();
    return new Version(elements);
  }

  applyStep(step: number): Version {
    if (step <= 0) {
      throw new Error('Step must be positive');
    }
    const elements = [...this.#inner];
    elements[elements.length - 1] += step;
    return new Version(elements);
  }
}

/**
 * A minimal set of incomparable versions.
 * 
 * This keeps the min antichain.
 */
export class Antichain {
  elements: Version[];

  constructor(elements: Version[]) {
    this.elements = [];
    for (const element of elements) {
      this.#insert(element);
    }
  }

  #insert(element: Version): void {
    for (const e of this.elements) {
      if (e.lessEqual(element)) {
        return;
      }
    }
    this.elements = this.elements.filter(x => !element.lessEqual(x));
    this.elements.push(element);
  }

  meet(other: Antichain): Antichain {
    const out = new Antichain([]);
    for (const element of this.elements) {
      out.#insert(element);
    }
    for (const element of other.elements) {
      out.#insert(element);
    }
    return out;
  }

  equals(other: Antichain): boolean {
    if (this.elements.length !== other.elements.length) {
      return false;
    }
    const sorted1 = [...this.elements].sort();
    const sorted2 = [...other.elements].sort();
    return sorted1.every((v, i) => v.equals(sorted2[i]));
  }

  lessThan(other: Antichain): boolean {
    return this.lessEqual(other) && !this.equals(other);
  }

  lessEqual(other: Antichain): boolean {
    for (const o of other.elements) {
      let lessEqual = false;
      for (const s of this.elements) {
        if (s.lessEqual(o)) {
          lessEqual = true;
          break;
        }
      }
      if (!lessEqual) {
        return false;
      }
    }
    return true;
  }

  lessEqualVersion(version: Version): boolean {
    for (const elem of this.elements) {
      if (elem.lessEqual(version)) {
        return true;
      }
    }
    return false;
  }

  isEmpty(): boolean {
    return this.elements.length === 0;
  }

  extend(): Antichain {
    const out = new Antichain([]);
    for (const elem of this.elements) {
      out.#insert(elem.extend());
    }
    return out;
  }

  truncate(): Antichain {
    const out = new Antichain([]);
    for (const elem of this.elements) {
      out.#insert(elem.truncate());
    }
    return out;
  }

  applyStep(step: number): Antichain {
    const out = new Antichain([]);
    for (const elem of this.elements) {
      out.#insert(elem.applyStep(step));
    }
    return out;
  }
} 