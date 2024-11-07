"""The implementation of index structures roughly analogous to differential arrangements for manipulating and
accessing (key, value) structured data across multiple versions (times).
"""

from collections import defaultdict
from collection import Collection
from order import Version, Antichain


class Index:
    """A map from a difference collection trace's keys -> versions at which
    the key has nonzero multiplicity -> (value, multiplicities) that changed.

    Used in operations like join and reduce where the operation needs to
    exploit the key-value structure of the data to run efficiently.

    This implementation supports the general case of partially ordered versions.
    """

    def __init__(self):
        self.inner = defaultdict(lambda: defaultdict(list))
        # TODO: take an initial time?
        self.compaction_frontier = None

    def _validate(self, requested_version):
        if self.compaction_frontier is None:
            return True
        if isinstance(requested_version, Antichain):
            assert self.compaction_frontier.less_equal(requested_version)
        elif isinstance(requested_version, Version):
            assert self.compaction_frontier.less_equal_version(requested_version)

    def reconstruct_at(self, key, requested_version):
        self._validate(requested_version)
        out = []
        for (version, values) in self.inner[key].items():
            if version.less_equal(requested_version):
                out.extend(values)
        return out

    def versions(self, key):
        return [version for version in self.inner[key].keys()]

    def add_value(self, key, version, value):
        self._validate(version)
        self.inner[key][version].append(value)

    def append(self, other):
        for (key, versions) in other.inner.items():
            for (version, data) in versions.items():
                self.inner[key][version].extend(data)

    def join(self, other):
        collections = defaultdict(list)
        for (key, versions) in self.inner.items():
            if key not in other.inner:
                continue
            other_versions = other.inner[key]

            for (version1, data1) in versions.items():
                for (version2, data2) in other_versions.items():
                    for (val1, mul1) in data1:
                        for (val2, mul2) in data2:
                            result_version = version1.join(version2)
                            collections[result_version].append(
                                ((key, (val1, val2)), mul1 * mul2)
                            )
        return [
            (version, Collection(c)) for (version, c) in collections.items() if c != []
        ]

    def compact(self, compaction_frontier, keys=[]):
        self._validate(compaction_frontier)

        def consolidate_values(values):
            consolidated = defaultdict(int)
            for (value, multiplicity) in values:
                consolidated[value] += multiplicity

            return [
                (value, multiplicity)
                for (value, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]

        if keys == []:
            keys = [key for key in self.inner.keys()]

        for key in keys:
            versions = self.inner[key]
            to_compact = [
                version
                for version in versions.keys()
                if compaction_frontier.less_equal_version(version) is not True
            ]
            to_consolidate = set()
            for version in to_compact:
                values = versions.pop(version)
                new_version = version.advance_by(compaction_frontier)
                versions[new_version].extend(values)
                to_consolidate.add(new_version)
            for version in to_consolidate:
                values = versions.pop(version)
                versions[version] = consolidate_values(values)
        assert self.compaction_frontier is None or self.compaction_frontier.less_equal(
            compaction_frontier
        )
        self.compaction_frontier = compaction_frontier


if __name__ == "__main__":
    # Basic operations tests
    index = Index()
    version1 = Version([1])
    version2 = Version([2])
    
    # # Test add and reconstruct values
    # index.add_value("key1", version1, (10, 1))
    # index.add_value("key1", version1, (20, 2))
    # assert index.reconstruct_at("key1", version1) == [(10, 1), (20, 2)]
    
    # # Test empty key reconstruction
    # assert index.reconstruct_at("nonexistent", version1) == []
    
    # # Test versions for a key
    # index.add_value("key1", version2, (20, 1))
    # versions = index.versions("key1")
    # assert len(versions) == 2
    # assert version1 in versions
    # assert version2 in versions
    
    # # Test append
    # other_index = Index()
    # other_index.add_value("key1", version1, (30, 1))
    # other_index.add_value("key2", version1, (40, 1))
    # index.append(other_index)
    # assert index.reconstruct_at("key1", version1) == [(10, 1), (20, 2), (30, 1)]
    # assert index.reconstruct_at("key2", version1) == [(40, 1)]
    
    # # Test join
    # join_index1 = Index()
    # join_index2 = Index()
    # join_index1.add_value("key1", version1, (10, 2))
    # join_index2.add_value("key1", version1, (20, 3))
    # result = join_index1.join(join_index2)
    # assert len(result) == 1
    # result_version, multiset = result[0]
    # assert result_version == version1
    # assert multiset._inner == [(("key1", (10, 20)), 6)]  # 2 * 3 = 6
    
    # Test compaction
    compact_index = Index()
    frontier = Antichain([Version([2])])
    compact_index.add_value("key1", version1, (10, 1))
    compact_index.add_value("key1", version1, (10, 2))
    compact_index.add_value("key1", version2, (10, -1))
    compact_index.compact(frontier)
    result = compact_index.reconstruct_at("key1", version2)
    assert sorted(result) == [(10, 2)]
    
    print("All tests passed!")