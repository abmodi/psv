package com.abmodi.psv;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class PsvReader implements DataSourceReader {
  private final StructType schema = new StructType().add("name", "string")
      .add("age", "int");

  public StructType readSchema() {
    return schema;
  }

  public List<InputPartition<InternalRow>> planInputPartitions() {
    InputPartition<InternalRow> partition1 = new PsvInputPartition(0, 2);
    InputPartition<InternalRow> partition2 = new PsvInputPartition(2, 3);
    return Arrays.asList(partition1, partition2);
  }
}
