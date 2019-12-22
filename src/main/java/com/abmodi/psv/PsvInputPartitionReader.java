package com.abmodi.psv;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.unsafe.types.UTF8String;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PsvInputPartitionReader implements InputPartitionReader<InternalRow> {

  private static Map<String, Integer> data = new HashMap<String, Integer>() {{
    put("Abhishek", 29);
    put("Prakhar", 27);
    put("Manju", 29);
  }};

  private final ArrayList<String> names;
  private final ArrayList<Integer> ages;

  private int endIndex;
  private int index;

  public PsvInputPartitionReader(int startIndex, int endIndex) {
    this.endIndex = endIndex;
    this.index = startIndex - 1;
    names = new ArrayList<String>(data.keySet());
    ages = new ArrayList<Integer>(data.values());
  }


  public boolean next() throws IOException {
    index += 1;
    return index < endIndex;
  }

  public InternalRow get() {
    try {
      return new GenericInternalRow(
          new Object[] { UTF8String.fromString(names.get(index)),
              ages.get(index) });
    } catch(Exception e) {
    }
    return null;
  }

  public void close() throws IOException {

  }
}
