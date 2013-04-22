package org.apache.hadoop.hive.jdbchandler;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class OracleDBRecordWriter extends DBRecordWriter {

  private static final Log LOG = LogFactory.getLog(OracleDBRecordWriter.class);

  protected OracleDBRecordWriter(Connection connection, String tableName, String[] fieldNames,
      boolean truncate, boolean replace) {
    super(connection, tableName, fieldNames, truncate, replace);
  }

}
