package com.abmodi.psv;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

public class PsvInputPartition implements InputPartition<InternalRow> {
  private final int startIndex;
  private final int endIndex;

  public PsvInputPartition(int startIndex, int endIndex) {
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public InputPartitionReader<InternalRow> createPartitionReader() {
    return new PsvInputPartitionReader(startIndex, endIndex);
  }

  public String[] preferredLocations() {
    return new String[0];
  }
}
