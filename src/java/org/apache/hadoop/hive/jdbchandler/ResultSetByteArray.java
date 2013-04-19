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
    private int type;
    private int dataLength;
    private byte[] rawData;
    public int getType() {
      return type;
    }
    public void setType(int type) {
      this.type = type;
    }
    public int getDataLength() {
      return dataLength;
    }
    public void setDataLength(int dataLength) {
      this.dataLength = dataLength;
    }
    public byte[] getRawData() {
      return rawData;
    }
    public void setRawData(byte[] rawData) {
      this.rawData = rawData;
    }

  }


}
