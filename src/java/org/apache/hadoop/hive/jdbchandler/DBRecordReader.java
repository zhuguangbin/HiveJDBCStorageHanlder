package org.apache.hadoop.hive.jdbchandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.jdbchandler.HiveDBInputFormat.DBInputSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * A RecordReader that reads records from a SQL table.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
public class DBRecordReader<T extends DBWritable> implements RecordReader<LongWritable, T> {

  private static final Log LOG = LogFactory.getLog(DBRecordReader.class);

  private final Connection connection;
  private final ResultSet results;
  protected PreparedStatement statement;
  private final Class<T> inputClass;
  private final Configuration conf;
  private final DBConfiguration dbConf;
  private final DBInputSplit split;
  private final String conditions;
  private final String[] fieldNames;
  private final String tableName;
  private long pos = 0;

  /**
   * @param split
   *          The InputSplit to read data for
   * @throws SQLException
   */
  protected DBRecordReader(DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConf,
      String cond, String [] fields, String table)
      throws SQLException {

    this.split = split;
    this.inputClass = inputClass;
    this.connection = conn;
    this.conf = conf;
    this.dbConf = dbConf;
    this.conditions = cond;
    this.fieldNames = fields;
    this.tableName = table;

    results = executeQuery(getSelectQuery());
  }

  protected ResultSet executeQuery(String query) throws SQLException {
    this.statement = connection.prepareStatement(query,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    return statement.executeQuery();
  }

  /**
   * Returns the query for selecting the records,
   * subclasses can override this for custom behaviour.
   */
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();

    if (dbConf.getInputQuery() == null) {
      query.append("SELECT ");

      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(", ");
        }
      }

      query.append(" FROM ").append(tableName);
      query.append(" AS ").append(tableName); // in hsqldb this is necessary
      if (conditions != null && conditions.length() > 0) {
        query.append(" WHERE (").append(conditions).append(")");
      }
      String orderBy = dbConf.getInputOrderBy();
      if (orderBy != null && orderBy.length() > 0) {
        query.append(" ORDER BY ").append(orderBy);
      }
    }
    else {
      query.append(dbConf.getInputQuery());
    }

    query.append(" LIMIT ").append(split.getLength());
    query.append(" OFFSET ").append(split.getStart());
    LOG.info("Select Query : " + query.toString());
    return query.toString();
  }

  /** {@inheritDoc} */
  public void close() throws IOException {
    try {
      connection.commit();
      results.close();
      statement.close();
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(StringUtils.stringifyException(e));
    }
  }

  /** {@inheritDoc} */
  public LongWritable createKey() {
    return new LongWritable();
  }

  /** {@inheritDoc} */
  public T createValue() {
    return ReflectionUtils.newInstance(inputClass, conf);
  }

  /** {@inheritDoc} */
  public long getPos() throws IOException {
    return pos;
  }

  /** {@inheritDoc} */
  public float getProgress() throws IOException {
    return pos / (float) split.getLength();
  }

  /** {@inheritDoc} */
  public boolean next(LongWritable key, T value) throws IOException {
    try {
      if (!results.next()) {
        return false;
      }

      // Set the key field value as the output key value
      key.set(pos + split.getStart());

      value.readFields(results);

      pos++;
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(StringUtils.stringifyException(e));
    }
    return true;
  }

  protected HiveDBInputFormat.DBInputSplit getSplit() {
    return split;
  }

  protected String [] getFieldNames() {
    return fieldNames;
  }

  protected String getTableName() {
    return tableName;
  }

  protected String getConditions() {
    return conditions;
  }

  protected DBConfiguration getDBConf() {
    return dbConf;
  }

  protected Connection getConnection() {
    return connection;
  }

  protected PreparedStatement getStatement() {
    return statement;
  }

  protected void setStatement(PreparedStatement stmt) {
    this.statement = stmt;
  }

}
