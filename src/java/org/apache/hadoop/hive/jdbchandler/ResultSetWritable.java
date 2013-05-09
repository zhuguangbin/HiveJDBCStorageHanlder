package org.apache.hadoop.hive.jdbchandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.jdbchandler.ResultSetByteArray.Column;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class ResultSetWritable implements DBWritable, Writable {

  private static final Log LOG = LogFactory.getLog(ResultSetWritable.class);

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

    ByteConverter converter = null;
    String dbProductName = stmt.getConnection().getMetaData().getDatabaseProductName().toUpperCase();
    if (dbProductName.startsWith("ORACLE")) {
      converter = new OracleByteConverter();
    }else if (dbProductName.startsWith("MYSQL")) {
      converter = new MySQLByteConverter();
    }else if (dbProductName.startsWith("POSTGRESQL")) {
      converter = new PostgresByteConverter();
    }else {
      throw new RuntimeException("Hive does not support writing to " + dbProductName);
    }

    ParameterMetaData paramMetaData = stmt.getParameterMetaData();
    for (int i = 1; i <= paramMetaData.getParameterCount(); i++) {

      int colTypeCode = rs.getColumns().get(i - 1).getType();
      byte[] colRawDataByteArray = rs.getColumns().get(i - 1).getRawData();
      String colDataString = new String(colRawDataByteArray);

      String colTypeClass = TypeBridge.toJavaType(colTypeCode);

      if (colTypeClass.equals(java.lang.Boolean.class.getCanonicalName())) {
        stmt.setBoolean(i, converter.toBoolean(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.Integer.class.getCanonicalName())) {
        stmt.setInt(i, converter.toInteger(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.Long.class.getCanonicalName())) {
        stmt.setLong(i, converter.toLong(colRawDataByteArray));
      } else if (colTypeClass.equals(java.math.BigDecimal.class.getCanonicalName())) {
        stmt.setBigDecimal(i, converter.toBigDecimal(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.Float.class.getCanonicalName())) {
        stmt.setFloat(i, converter.toFloat(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.Double.class.getCanonicalName())) {
        stmt.setDouble(i, converter.toDouble(colRawDataByteArray));
      } else if (colTypeClass.equals(java.lang.String.class.getCanonicalName())) {
        stmt.setString(i, converter.toString(colRawDataByteArray));
      } else if (colTypeClass.equals(java.sql.Date.class.getCanonicalName())) {
        stmt.setDate(i, converter.toDate(colRawDataByteArray));
      } else if (colTypeClass.equals(java.sql.Time.class.getCanonicalName())) {
        stmt.setTime(i, converter.toTime(colRawDataByteArray));
      } else if (colTypeClass.equals(java.sql.Timestamp.class.getCanonicalName())) {
        stmt.setTimestamp(i, converter.toTimestamp(colRawDataByteArray));
      } else {
        stmt.setBytes(i, colRawDataByteArray);
      }
    }
  }

  @Override
  public void readFields(ResultSet resultSet) throws SQLException {

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
