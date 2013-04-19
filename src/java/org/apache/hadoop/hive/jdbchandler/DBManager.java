package org.apache.hadoop.hive.jdbchandler;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.jdbchandler.JDBCSerDe.ColumnMapping;
import org.apache.hadoop.util.StringUtils;


public class DBManager {

  public static final Log LOG = LogFactory.getLog(DBManager.class);

  private final Configuration conf;
  private Connection connection;

  public DBManager(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Returns a connection object o the DB
   *
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public Connection getConnection() {
    if (connection == null) {
      try {
        DBConfiguration dbconf = new DBConfiguration(conf);
        connection = dbconf.getConnection();
      } catch (ClassNotFoundException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(StringUtils.stringifyException(e));
      } catch (SQLException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(StringUtils.stringifyException(e));
      }
    }
    return connection;
  }


  public boolean exists(String tableName) {
    try {
      Connection con = getConnection();
      DatabaseMetaData dbm = con.getMetaData();

      ResultSet tables = dbm.getTables(null, null, tableName, null);
      if (tables.next()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Table " + tableName + "exists.");
        }
        return true;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Table " + tableName + " does not exist.");
        }
        return false;
      }

    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(StringUtils.stringifyException(e));
    }
  }

  public boolean createTable(String tableName, List<ColumnMapping> columnMappings) {

    StringBuilder query = new StringBuilder();
    query.append(" CREATE TABLE ").append(tableName).append(" (");
    for (ColumnMapping columnMapping : columnMappings) {
      query.append(columnMapping.columnName).append(" ").append(columnMapping.columnType)
          .append("(").append(columnMapping.columnLength).append(")")
          .append(" COMMENT '").append(columnMapping.columnComments).append("',");
    }
    query.replace(query.lastIndexOf(","), query.lastIndexOf(",") + 1, ")");

    if (LOG.isDebugEnabled()) {
      LOG.debug(query.toString());
    }

    boolean exitStatus = false;
    Connection con = getConnection();
    try {
      Statement stmt = con.createStatement();
      exitStatus = stmt.execute(query.toString());
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(StringUtils.stringifyException(e));
    }

    return exitStatus;

  }

  public boolean dropTable(String tableName) {

    StringBuilder query = new StringBuilder();
    query.append(" DROP TABLE ").append(tableName);

    if (LOG.isDebugEnabled()) {
      LOG.debug(query.toString());
    }

    boolean exitStatus = false;
    Connection con = getConnection();
    try {
      Statement stmt = con.createStatement();
      exitStatus = stmt.execute(query.toString());
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(StringUtils.stringifyException(e));
    }

    return exitStatus;

  }

  public boolean truncateTable(String tableName) {

    StringBuilder query = new StringBuilder();
    query.append(" TRUNCATE TABLE ").append(tableName);

    if (LOG.isDebugEnabled()) {
      LOG.debug(query.toString());
    }

    boolean exitStatus = false;
    Connection con = getConnection();
    try {
      Statement stmt = con.createStatement();
      exitStatus = stmt.execute(query.toString());
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(StringUtils.stringifyException(e));
    }

    return exitStatus;

  }

}
