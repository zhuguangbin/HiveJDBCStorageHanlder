package org.apache.hadoop.hive.jdbchandler;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class MySQLDBRecordWriter extends DBRecordWriter {

  private static final Log LOG = LogFactory.getLog(MySQLDBRecordWriter.class);

  protected MySQLDBRecordWriter(Connection connection, String tableName, String[] fieldNames,
      boolean truncate, boolean replace, int batchSize) {
    super(connection, tableName, fieldNames, truncate, replace, batchSize);
  }


  @Override
  protected String constructQuery(String table, String[] fieldNames, boolean replace) {
    if (fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    StringBuilder query = new StringBuilder();

    if (replace) {
      query.append("REPLACE INTO ").append(table);
    }else {
      query.append("INSERT INTO ").append(table);
    }

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

    LOG.info("Insert Query: " + query.toString());

    return query.toString();
  }

}
