package org.apache.hadoop.hive.jdbchandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hive.jdbchandler.HiveDBInputFormat.DBInputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class MySQLDBRecordReader extends DBRecordReader<DBWritable> {

  protected MySQLDBRecordReader(DBInputSplit split, Class inputClass, JobConf job,
      Connection conn, DBConfiguration dbConf, String cond, String[] fields, String table)
      throws SQLException {
    super(split, inputClass, job, conn, dbConf, cond, fields, table);
    // TODO Auto-generated constructor stub
  }

//Execute statements for mysql in unbuffered mode.
 @Override
protected ResultSet executeQuery(String query) throws SQLException {
   statement = getConnection().prepareStatement(query,
     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//   statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
   return statement.executeQuery();
 }

}
