package org.apache.hadoop.hive.jdbchandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.jdbchandler.ResultSetByteArray.Column;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class ResultSetWritable implements DBWritable, Writable {

  private ResultSetByteArray rs = new ResultSetByteArray();

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub\
    int columnCount = in.readInt();
    rs.setColumnCount(columnCount);

    List<Column> columns = new ArrayList<Column>();
    for (int i = 1; i <= columnCount; i++) {
      ResultSetByteArray.Column column = rs.new Column();
      int columnLength = in.readInt();
      byte[] rawData = new byte[columnLength];
      in.readFully(rawData, 0, columnLength);
      column.setLength(columnLength);
      column.setRawData(rawData);

      columns.add(column);
    }

    rs.setColumns(columns);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    int columnCount = rs.getColumnCount();
    out.writeInt(columnCount);

    for (int i = 1; i <= columnCount; i++) {
      out.writeInt(rs.getColumns().get(i - 1).getLength());
      out.write(rs.getColumns().get(i - 1).getRawData());
    }


  }

  @Override
  public void write(PreparedStatement statement) throws SQLException {
    // TODO Auto-generated method stub
  }

  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    // TODO Auto-generated method stub
    int colCount = resultSet.getMetaData().getColumnCount();
    rs.setColumnCount(colCount);

    List<Column> columns = new ArrayList<Column>();

    for (int i = 1; i <= colCount; i++) {
      ResultSetByteArray.Column column = rs.new Column();
      byte[] colBytes = resultSet.getBytes(i);
      column.setLength(colBytes.length);
      column.setRawData(colBytes);

      columns.add(column);

    }

    rs.setColumns(columns);

  }

  public ResultSetByteArray getResultSetByteArray() {
    return rs;
  }

  public void setResultSetByteArray(ResultSetByteArray rs) {
    this.rs = rs;
  }

}
