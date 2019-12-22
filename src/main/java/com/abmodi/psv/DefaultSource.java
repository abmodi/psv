package com.abmodi.psv;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

public class DefaultSource implements DataSourceV2, ReadSupport {
  public DataSourceReader createReader(DataSourceOptions dataSourceOptions) {
    return new PsvReader();
  }

  public DataSourceReader createReader(StructType schema,
      DataSourceOptions options) {
    return new PsvReader();
  }
}
