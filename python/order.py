"""The implementation of partially ordered versions (times) for use within a differential dataflow.
"""


class Version:
    """A partially, or totally ordered version (time), consisting of a tuple of
    integers.

    All versions within a scope of a dataflow must have the same dimension/number
    of coordinates. One dimensional versions are totally ordered. Multidimensional
    versions are partially ordered by the product partial order.
    """

    def __init__(self, version):
        if isinstance(version, int):
            assert version >= 0
            self.inner = (version,)
        elif isinstance(version, list) or isinstance(version, tuple):
            for i in version:
                assert isinstance(i, int)
                assert i >= 0
            self.inner = tuple(version)
        else:
            assert 0 > 1

    def __repr__(self):
        return f"Version({self.inner})"

    def __eq__(self, other):
        return self.inner == other.inner

    # The less than implementation used to sort versions must respect the partial
    # order (important for reduce).
    def __lt__(self, other):
        return self.inner.__lt__(other.inner)

    def __hash__(self):
        return hash(self.inner)

    def _validate(self, other):
        assert len(self.inner) > 0
        assert len(self.inner) == len(other.inner)

    def less_equal(self, other):
        self._validate(other)

        for (i1, i2) in zip(self.inner, other.inner):
            if i1 > i2:
                return False
        return True

    def less_than(self, other):
        if self.less_equal(other) is True and self.inner != other.inner:
            return True
        return False

    def join(self, other):
        self._validate(other)
        out = []

        for (i1, i2) in zip(self.inner, other.inner):
            out.append(max(i1, i2))
        return Version(out)

    def meet(self, other):
        self._validate(other)
        out = []

        for (i1, i2) in zip(self.inner, other.inner):
            out.append(min(i1, i2))
        return Version(out)

    # TODO the proof for this is in the sharing arrangements paper.
    def advance_by(self, frontier):
        if frontier.inner == ():
            return self
        result = self.join(frontier.inner[0])
        for elem in frontier.inner:
            result = result.meet(self.join(elem))
        return result

    def extend(self):
        elements = [e for e in self.inner]
        elements.append(0)
        return Version(elements)

    def truncate(self):
        elements = [e for e in self.inner]
        elements.pop()
        return Version(elements)

    def apply_step(self, step):
        assert step > 0
        elements = [e for e in self.inner]
        elements[-1] += step
        return Version(elements)


# This keeps the min antichain.
# I fully stole this from frank. TODO: Understand this better
class Antichain:
    """A minimal set of incomparable versions."""

    def __init__(self, elements):
        self.inner = []
        for element in elements:
            self._insert(element)

    def __repr__(self):
        return f"Antichain({self.inner})"

    def _insert(self, element):
        for e in self.inner:
            if e.less_equal(element):
                return
        self.inner = [x for x in self.inner if element.less_equal(x) is not True]
        self.inner.append(element)

    # TODO: is it true that the set of versions <= meet(x, y) is the intersection of the set of versions <= x and the set of versions <= y?
    def meet(self, other):
        out = Antichain([])
        for element in self.inner:
            out._insert(element)
        for element in other.inner:
            out._insert(element)

        return out

    def _equals(self, other):
        elements_1 = [x for x in self.inner]
        elements_2 = [y for y in other.inner]

        if len(elements_1) != len(elements_2):
            return False
        elements_1.sort()
        elements_2.sort()

        for (x, y) in zip(elements_1, elements_2):
            if x != y:
                return False
        return True

    # Returns true if other dominates self
    # in other words self < other means
    # self <= other AND self != other
    def less_than(self, other):
        if self.less_equal(other) is not True:
            return False

        if self._equals(other):
            return False

        return True

    def less_equal(self, other):
        for o in other.inner:
            less_equal = False
            for s in self.inner:
                if s.less_equal(o):
                    less_equal = True
            if less_equal == False:
                return False
        return True

    def less_equal_version(self, version):
        for elem in self.inner:
            if elem.less_equal(version):
                return True
        return False

    def extend(self):
        out = Antichain([])
        for elem in self.inner:
            out._insert(elem.extend())
        return out

    def truncate(self):
        out = Antichain([])
        for elem in self.inner:
            out._insert(elem.truncate())
        return out

    def apply_step(self, step):
        out = Antichain([])
        for elem in self.inner:
            out._insert(elem.apply_step(step))
        return out

    def _elements(self):
        return [x for x in self.inner]


if __name__ == "__main__":

    v0_0 = Version([0, 0])
    v1_0 = Version([1, 0])
    v0_1 = Version([0, 1])
    v1_1 = Version([1, 1])
    v2_0 = Version([2, 0])

    assert v0_0.less_than(v1_0)
    assert v0_0.less_than(v0_1)
    assert v0_0.less_than(v1_1)
    assert v0_0.less_equal(v1_0)
    assert v0_0.less_equal(v0_1)
    assert v0_0.less_equal(v1_1)

    assert v1_0.less_than(v1_0) is not True
    assert v1_0.less_equal(v1_0)
    assert v1_0.less_equal(v0_1) is not True
    assert v0_1.less_equal(v1_0) is not True
    assert v0_1.less_equal(v1_1)
    assert v1_0.less_equal(v1_1)
    assert v0_0.less_equal(v1_1)

    assert Antichain([v0_0]).less_equal(Antichain([v1_0]))
    assert Antichain([v0_0])._equals(Antichain([v1_0])) is not True
    assert Antichain([v0_0]).less_than(Antichain([v1_0]))
    assert Antichain([v2_0, v1_1]).less_than(Antichain([v2_0]))
