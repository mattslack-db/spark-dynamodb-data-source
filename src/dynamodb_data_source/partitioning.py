"""Partitioning utilities for DynamoDB parallel scan."""

from pyspark.sql.datasource import InputPartition


class SegmentPartition(InputPartition):
    """
    Represents a DynamoDB parallel scan segment.

    DynamoDB Scan supports parallel reads by splitting the table into
    segments. Each partition corresponds to one segment of the scan.
    """

    def __init__(self, segment, total_segments):
        """
        Initialize a segment partition.

        Args:
            segment: Segment number (0 to total_segments - 1)
            total_segments: Total number of parallel scan segments
        """
        self.segment = segment
        self.total_segments = total_segments

    def __eq__(self, other):
        """Check equality based on partition content."""
        if not isinstance(other, SegmentPartition):
            return False
        return self.segment == other.segment and self.total_segments == other.total_segments

    def __hash__(self):
        """Return hash for use in sets/dicts."""
        return hash((self.segment, self.total_segments))

    def __repr__(self):
        """Return string representation."""
        return f"SegmentPartition(segment={self.segment}, total_segments={self.total_segments})"
