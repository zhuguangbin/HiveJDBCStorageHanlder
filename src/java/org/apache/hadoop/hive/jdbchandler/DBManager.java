package org.apache.hadoop.hive.jdbchandler;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.jdbchandler.JDBCSerDe.ColumnMapping;


public class DBManager {

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
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
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
        return true;
      } else {
        return false;
      }

    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  public boolean createTable(String tableName, List<ColumnMapping> columnMappings) {

    StringBuilder query = new StringBuilder();
    query.append(" CREATE TABLE ").append(tableName).append(" (");
    for (ColumnMapping columnMapping : columnMappings) {
      query.append(columnMapping.columnName).append(" ").append(columnMapping.columnType)
          .append(" COMMENT '").append(columnMapping.columnComments).append("',");
    }
    query.replace(query.lastIndexOf(","), query.lastIndexOf(",") + 1, ")");


    boolean exitStatus = false;
    Connection con = getConnection();
    try {
      Statement stmt = con.createStatement();
      exitStatus = stmt.execute(query.toString());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      exitStatus = false;
    }

    return exitStatus;

  }

  public boolean dropTable(String tableName) {

    StringBuilder query = new StringBuilder();
    query.append(" DROP TABLE ").append(tableName);

    boolean exitStatus = false;
    Connection con = getConnection();
    try {
      Statement stmt = con.createStatement();
      exitStatus = stmt.execute(query.toString());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      exitStatus = false;
    }

    return exitStatus;

  }

}
