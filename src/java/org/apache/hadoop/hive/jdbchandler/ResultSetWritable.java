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
    LOG.info("---------readFields(DataInput in) start ---------------");
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

      LOG.info("column "+ i +", colTypeCode: " + type +", data bytes length: "+ dataLength + ", data bytes: "+ rawData);

      columns.add(column);
    }
    LOG.info("---------readFields(DataInput in) end---------------");

    rs.setColumns(columns);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    LOG.info("---------write(DataOutput out) start---------------");
    int columnCount = rs.getColumnCount();
    out.writeInt(columnCount);

    for (int i = 1; i <= columnCount; i++) {
      out.writeInt(rs.getColumns().get(i - 1).getType());
      out.writeInt(rs.getColumns().get(i - 1).getDataLength());
      out.write(rs.getColumns().get(i - 1).getRawData());
      LOG.info("column "+ i +", colTypeCode: " + rs.getColumns().get(i - 1).getType() +", data bytes length: "+ rs.getColumns().get(i - 1).getDataLength() + ", data bytes: " + rs.getColumns().get(i - 1).getRawData());
    }

    LOG.info("---------write(DataOutput out) end---------------");

  }

  @Override
  public void write(PreparedStatement stmt) throws SQLException {

    LOG.info("---------write(PreparedStatement stmt) start---------------");

    ParameterMetaData paramMetaData = stmt.getParameterMetaData();
    for (int i = 1; i <= paramMetaData.getParameterCount(); i++) {

      int colTypeCode = rs.getColumns().get(i - 1).getType();
      byte[] colRawDataByteArray = rs.getColumns().get(i - 1).getRawData();
      String colDataString = new String(colRawDataByteArray);

      String colTypeClass = SQL2JavaTypeBridge.toJavaType(colTypeCode);

      LOG.info("column "+ i +", colTypeCode: " + colTypeCode +", colTypeClass: " + colTypeClass +", data bytes length: "+ colRawDataByteArray.length + ", data bytes: "+ colRawDataByteArray);

      if (colTypeClass.equals(java.lang.Boolean.class.getCanonicalName())) {
        stmt.setBoolean(i, Boolean.parseBoolean(colDataString));
      } else if (colTypeClass.equals(java.lang.Integer.class.getCanonicalName())) {
        stmt.setInt(i, Integer.parseInt(colDataString));
      } else if (colTypeClass.equals(java.lang.Long.class.getCanonicalName())) {
        stmt.setLong(i, Long.parseLong(colDataString));
      } else if (colTypeClass.equals(java.math.BigDecimal.class.getCanonicalName())) {
        stmt.setBigDecimal(i, BigDecimal.valueOf(Long.parseLong(colDataString)));
      } else if (colTypeClass.equals(java.lang.Float.class.getCanonicalName())) {
        stmt.setFloat(i, Float.parseFloat(colDataString));
      } else if (colTypeClass.equals(java.lang.Double.class.getCanonicalName())) {
        stmt.setDouble(i, Double.parseDouble(colDataString));
      } else if (colTypeClass.equals(java.lang.String.class.getCanonicalName())) {
        stmt.setString(i, colDataString);
      } else if (colTypeClass.equals(java.sql.Date.class.getCanonicalName())) {
        stmt.setDate(i, java.sql.Date.valueOf(colDataString));
      } else if (colTypeClass.equals(java.sql.Time.class.getCanonicalName())) {
        stmt.setTime(i, java.sql.Time.valueOf(colDataString));
      } else if (colTypeClass.equals(java.sql.Timestamp.class.getCanonicalName())) {
        stmt.setTimestamp(i, java.sql.Timestamp.valueOf(colDataString));
      } else {
        stmt.setBytes(i, colRawDataByteArray);
      }
    }
    LOG.info("---------write(PreparedStatement stmt) end---------------");
  }

  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    LOG.info("---------readFields(ResultSet resultSet) start---------------");

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

      LOG.info("column "+ i +", colTypeCode: " + type +", data bytes length: "+ colBytes.length + ", data bytes: " + colBytes);

      columns.add(column);

    }

    rs.setColumns(columns);
    LOG.info("---------readFields(ResultSet resultSet) end---------------");

  }

  public ResultSetByteArray getResultSetByteArray() {
    return rs;
  }

  public void setResultSetByteArray(ResultSetByteArray rs) {
    this.rs = rs;
  }

}
