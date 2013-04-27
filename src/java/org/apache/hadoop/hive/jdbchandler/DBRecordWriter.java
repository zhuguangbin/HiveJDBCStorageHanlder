package org.apache.hadoop.hive.jdbchandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;


/**
 * A RecordWriter that writes the reduce output to a SQL table
 */
public class DBRecordWriter implements RecordWriter {

  private static final Log LOG = LogFactory.getLog(DBRecordWriter.class);

  protected final Connection connection;
  protected final PreparedStatement statement;
  protected int batchSize = 1000;
  private int count = 0;

  protected DBRecordWriter(Connection connection, String tableName, String[] fieldNames,
      boolean truncate, boolean replace) {

    try {
      this.connection = connection;
      this.statement = connection.prepareStatement(constructQuery(tableName, fieldNames, replace));
      // NOTE: truncate operation is not in a transaction, when when insert exception, cannot
      // rollback.
      if (truncate) {
        this.statement.addBatch("TRUNCATE TABLE " + tableName);
      }
      this.connection.setAutoCommit(false);
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(StringUtils.stringifyException(e));
    }
  }

  @Override
  public void write(Writable w) throws IOException {
    ResultSetWritable rs = (ResultSetWritable) w;

    try {
      rs.write(statement);
      statement.addBatch();
      if (++count % batchSize == 0) {
        LOG.info("statment added "+ count + " record. now trying to executeBatch");
        statement.executeBatch();
        LOG.info("statment writed "+ count + " record successfully.");
      }
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(StringUtils.stringifyException(e));
    }

  }

  @Override
  public void close(boolean abort) throws IOException {
    // TODO Auto-generated method stub
    try {
      statement.executeBatch();
      LOG.info("all "+count+" records written to db. now trying to commit.");
      connection.commit();
      LOG.info("all "+count+" records written to db successfully. ");
    } catch (SQLException e) {
      try {
        LOG.info("exception!!. now trying to rollback.");
        connection.rollback();
      } catch (SQLException ex) {
        LOG.error(StringUtils.stringifyException(ex));
        throw new IOException(StringUtils.stringifyException(ex));
      }
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(StringUtils.stringifyException(e));
    } finally {
      try {
        LOG.info("finally, close statment and connection.");
        statement.close();
        connection.close();
        LOG.info("statment and connection closed successfully.");
      } catch (SQLException ex) {
        LOG.error(StringUtils.stringifyException(ex));
        throw new IOException(StringUtils.stringifyException(ex));
      }
    }
  }

  /**
   * Constructs the query used as the prepared statement to insert data.
   *
   * @param table
   *          the table to insert into
   * @param fieldNames
   *          the fields to insert into. If field names are unknown, supply an
   *          array of nulls.
   */
  protected String constructQuery(String table, String[] fieldNames, boolean replace) {
    if (fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    // default DB does not support replace when row exists.

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table);

    if (fieldNames.length > 0 && fieldNames[0] != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(",");
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if (i != fieldNames.length - 1) {
        query.append(",");
      }
    }
    query.append(");");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Insert Query: " + query.toString());
    }

    return query.toString();
  }
}
