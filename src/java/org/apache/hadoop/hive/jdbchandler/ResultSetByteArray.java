package org.apache.hadoop.hive.jdbchandler;

import java.util.List;


public class ResultSetByteArray {

  private int columnCount;

  private List<Column> columns;


  public int getColumnCount() {
    return columnCount;
  }

  public void setColumnCount(int columnCount) {
    this.columnCount = columnCount;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  class Column {
    private int length;
    private byte[] rawData;

    public int getLength() {
      return length;
    }

    public void setLength(int length) {
      this.length = length;
    }

    public byte[] getRawData() {
      return rawData;
    }

    public void setRawData(byte[] rawData) {
      this.rawData = rawData;
    }

  }


}
