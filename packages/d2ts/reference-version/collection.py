"""The implementation of collections (multisets) of data and functional operations over single collections.
"""

from collections import defaultdict


class Collection:
    """A multiset of data"""

    def __init__(self, dataz=None):
        if dataz is None:
            dataz = []
        self._inner = dataz

    def __repr__(self):
        return f"Collection({self._inner})"

    def map(self, f):
        """Apply a function to all records in the collection."""
        return Collection(
            [(f(data), multiplicity) for (data, multiplicity) in self._inner]
        )

    def filter(self, f):
        """Filter out records for which a function f(record) evaluates to False."""
        return Collection(
            [
                (data, multiplicity)
                for (data, multiplicity) in self._inner
                if f(data) == True
            ]
        )

    def negate(self):
        return Collection(
            [(data, -multiplicity) for (data, multiplicity) in self._inner]
        )

    def concat(self, other):
        """Concatenate two collections together."""
        out = []
        out.extend(self._inner)
        out.extend(other._inner)
        return Collection(out)

    def consolidate(self):
        """Produce as output a collection that is logically equivalent to the input
        but which combines identical instances of the same record into one
        (record, multiplicity) pair.
        """
        consolidated = defaultdict(int)
        for (data, multiplicity) in self._inner:
            consolidated[data] += multiplicity
        consolidated = [
            (data, multiplicity)
            for (data, multiplicity) in consolidated.items()
            if multiplicity != 0
        ]
        consolidated.sort()
        return Collection(consolidated)

    def join(self, other):
        """Match pairs (k, v1) and (k, v2) from the two input collections and produce (k, (v1, v2))."""
        out = []
        for ((k1, v1), d1) in self._inner:
            for ((k2, v2), d2) in other._inner:
                if k1 == k2:
                    out.append(((k1, (v1, v2)), d1 * d2))
        return Collection(out)

    def reduce(self, f):
        """Apply a reduction function to all record values, grouped by key."""
        keys = defaultdict(list)
        out = []
        for ((key, val), multiplicity) in self._inner:
            keys[key].append((val, multiplicity))
        for (key, vals) in keys.items():
            results = f(vals)
            for (val, multiplicity) in results:
                out.append(((key, val), multiplicity))
        return Collection(out)

    def count(self):
        """Count the number of times each key occurs in the collection."""

        def count_inner(vals):
            out = 0
            for (_, multiplicity) in vals:
                out += multiplicity
            return [(out, 1)]

        return self.reduce(count_inner)

    def sum(self):
        """Produce the sum of all the values paired with a key, for all keys in the collection."""

        def sum_inner(vals):
            out = 0
            for (val, multiplicity) in vals:
                out += val * multiplicity
            return [(out, 1)]

        return self.reduce(sum_inner)

    def min(self):
        """Produce the minimum value associated with each key in the collection.

        Note that no record may have negative multiplicity when computing the min,
        as it is unclear what exactly the minimum record is in that case.
        """

        def min_inner(vals):
            consolidated = defaultdict(int)
            for (val, multiplicity) in vals:
                consolidated[val] += multiplicity
            vals = [
                (val, multiplicity)
                for (val, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]
            if len(vals) != 0:
                out = vals[0][0]
                for (val, multiplicity) in vals:
                    assert multiplicity > 0
                    if val < out:
                        out = val
                return [(out, 1)]
            else:
                return []

        return self.reduce(min_inner)

    def max(self):
        """Produce the maximum value associated with each key in the collection.

        Note that no record may have negative multiplicity when computing the max,
        as it is unclear what exactly the maximum record is in that case.
        """

        def max_inner(vals):
            consolidated = defaultdict(int)
            for (val, multiplicity) in vals:
                consolidated[val] += multiplicity
            vals = [
                (val, multiplicity)
                for (val, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]
            if len(vals) != 0:
                out = vals[0][0]
                for (val, multiplicity) in vals:
                    assert multiplicity > 0
                    if val > out:
                        out = val
                return [(out, 1)]
            else:
                return []

        return self.reduce(max_inner)

    def distinct(self):
        """Reduce the collection to a set of elements (from a multiset).

        Note that no record may have negative multiplicity when producing this set,
        as elements of sets may only have multiplicity one, and it is unclear that is
        an appropriate output for elements with negative multiplicity.
        """

        def distinct_inner(vals):
            consolidated = defaultdict(int)
            for (val, multiplicity) in vals:
                consolidated[val] += multiplicity
            vals = [
                (val, multiplicity)
                for (val, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]
            for (val, multiplicity) in vals:
                assert multiplicity > 0
            return [(val, 1) for (val, _) in vals]

        return self.reduce(distinct_inner)

    def iterate(self, f):
        """Repeatedly invoke a function f on a collection, and return the result
        of applying the function an infinite number of times (fixedpoint).

        Note that if the function does not converge to a fixedpoint this implementation
        will run forever.
        """
        curr = Collection(self._inner)
        while True:
            result = f(curr)
            if result._inner == curr._inner:
                break
            curr = result
        return curr

    def _extend(self, other):
        self._inner.extend(other._inner)


if __name__ == "__main__":
    a = Collection([(("apple", "$5"), 2), (("banana", "$2"), 1)])
    b = Collection(
        [
            (("apple", "$3"), 1),
            (("apple", ("granny smith", "$2")), 1),
            (("kiwi", "$2"), 1),
        ]
    )
    c = Collection([(("apple", "$5"), 2), (("banana", "$2"), 1), (("apple", "$2"), 20)])
    d = Collection(
        [(("apple", 11), 1), (("apple", 3), 2), (("banana", 2), 3), (("coconut", 3), 1)]
    )
    e = Collection([(1, 1)])

    print(a.concat(b))
    print(a.join(b))
    print(b.join(a))
    print(a.filter(lambda data: data[0] != "apple"))
    print(a.map(lambda data: (data[1], data[0])))
    print(a.concat(b).count())
    print(a.concat(b).distinct())
    print(c.min())
    print(c.max())
    print(d.sum())

    def add_one(collection):
        return (
            collection.map(lambda data: data + 1)
            .concat(collection)
            .filter(lambda data: data <= 5)
            .map(lambda data: (data, ()))
            .distinct()
            .map(lambda data: data[0])
            .consolidate()
        )

    result = e.iterate(add_one).map(lambda data: (data, data * data))
    print(result)
