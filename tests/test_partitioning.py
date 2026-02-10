"""Tests for DynamoDB segment-based partitioning."""


def test_segment_partition_creation():
    """Test creating a segment partition."""
    from dynamodb_data_source.partitioning import SegmentPartition

    partition = SegmentPartition(segment=2, total_segments=4)

    assert partition.segment == 2
    assert partition.total_segments == 4


def test_segment_partition_equality():
    """Test partition equality comparison."""
    from dynamodb_data_source.partitioning import SegmentPartition

    p1 = SegmentPartition(0, 4)
    p2 = SegmentPartition(0, 4)
    p3 = SegmentPartition(1, 4)

    assert p1 == p2
    assert p1 != p3


def test_segment_partition_hash():
    """Test partition can be used in sets/dicts."""
    from dynamodb_data_source.partitioning import SegmentPartition

    p1 = SegmentPartition(0, 4)
    p2 = SegmentPartition(0, 4)
    p3 = SegmentPartition(1, 4)

    s = {p1, p2, p3}
    assert len(s) == 2


def test_segment_partition_repr():
    """Test partition string representation."""
    from dynamodb_data_source.partitioning import SegmentPartition

    partition = SegmentPartition(2, 4)
    assert "segment=2" in repr(partition)
    assert "total_segments=4" in repr(partition)


def test_segment_partition_not_equal_to_other_type():
    """Test partition is not equal to non-partition objects."""
    from dynamodb_data_source.partitioning import SegmentPartition

    partition = SegmentPartition(0, 1)
    assert partition != "not a partition"
    assert partition != 42
