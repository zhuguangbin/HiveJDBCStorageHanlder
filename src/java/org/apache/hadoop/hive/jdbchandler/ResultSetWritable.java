package org.apache.hadoop.hive.jdbchandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ParameterMetaData;
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
      int type = in.readInt();
      int dataLength = in.readInt();
      byte[] rawData = new byte[dataLength];
      in.readFully(rawData, 0, dataLength);
      column.setType(type);
      column.setDataLength(dataLength);
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
      out.writeInt(rs.getColumns().get(i - 1).getType());
      out.writeInt(rs.getColumns().get(i - 1).getDataLength());
      out.write(rs.getColumns().get(i - 1).getRawData());
    }


  }

  @Override
  public void write(PreparedStatement stmt) throws SQLException {

    ParameterMetaData paramMetaData = stmt.getParameterMetaData();
    for (int i = 1; i <= paramMetaData.getParameterCount(); i++) {

      String colTypeClass = paramMetaData.getParameterClassName(i);
      int colTypeCode = paramMetaData.getParameterType(i);
      String colTypeName = paramMetaData.getParameterTypeName(i);

      byte[] colRawDataByteArray = rs.getColumns().get(i - 1).getRawData();

      if (colTypeClass.equals(java.lang.Boolean.class.getCanonicalName())) {
        stmt.setBoolean(i, ByteArray2PrimitiveBridge.toBoolean(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.Integer.class.getCanonicalName())) {
        stmt.setInt(i, ByteArray2PrimitiveBridge.toInt(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.Long.class.getCanonicalName())) {
        stmt.setLong(i, ByteArray2PrimitiveBridge.toLong(colRawDataByteArray));
      } else if (colTypeClass.equals(java.math.BigInteger.class.getCanonicalName())) {
        stmt.setLong(i, ByteArray2PrimitiveBridge.toLong(colRawDataByteArray));
      } else if (colTypeClass.equals(java.math.BigDecimal.class.getCanonicalName())) {
        stmt.setBigDecimal(i, BigDecimal.valueOf(ByteArray2PrimitiveBridge.toLong(colRawDataByteArray)));
      } else if (colTypeClass.equals(java.lang.Float.class.getCanonicalName())) {
        stmt.setFloat(i, ByteArray2PrimitiveBridge.toFloat(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.Double.class.getCanonicalName())) {
        stmt.setDouble(i, ByteArray2PrimitiveBridge.toDouble(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.String.class.getCanonicalName())) {
        stmt.setString(i, ByteArray2PrimitiveBridge.toString(colRawDataByteArray));
      } else if (colTypeClass.equals(java.sql.Date.class.getCanonicalName())) {
        stmt.setDate(i, java.sql.Date.valueOf(ByteArray2PrimitiveBridge.toString(colRawDataByteArray)));
      } else if (colTypeClass.equals(java.sql.Time.class.getCanonicalName())) {
        stmt.setTime(i, java.sql.Time.valueOf(ByteArray2PrimitiveBridge.toString(colRawDataByteArray)));
      } else if (colTypeClass.equals(java.sql.Timestamp.class.getCanonicalName())) {
        stmt.setTimestamp(i, java.sql.Timestamp.valueOf(ByteArray2PrimitiveBridge.toString(colRawDataByteArray)));
      } else {
        stmt.setBytes(i, colRawDataByteArray);
      }
    }
  }

  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    // TODO Auto-generated method stub
    int colCount = resultSet.getMetaData().getColumnCount();
    rs.setColumnCount(colCount);

    List<Column> columns = new ArrayList<Column>();

    for (int i = 1; i <= colCount; i++) {
      ResultSetByteArray.Column column = rs.new Column();

      int type = resultSet.getMetaData().getColumnType(i);
      column.setType(type);

      byte[] colBytes = resultSet.getBytes(i);
      column.setDataLength(colBytes.length);
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
