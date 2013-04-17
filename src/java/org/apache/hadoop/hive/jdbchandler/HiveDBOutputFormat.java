/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.jdbchandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p>
 * {@link HiveDBOutputFormat} accepts &lt;key,value&gt; pairs, where key has a type extending
 * DBWritable. Returned {@link RecordWriter} writes <b>only the key</b> to the database with a batch
 * SQL query.
 *
 */
public class HiveDBOutputFormat<K extends Writable, V extends ResultSetWritable> implements HiveOutputFormat<K, V>,
    OutputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(HiveDBOutputFormat.class);

  /**
   * A RecordWriter that writes the reduce output to a SQL table
   */
  protected class DBRecordWriter
      implements RecordWriter {

    private final Connection connection;
    private final PreparedStatement statement;

    protected DBRecordWriter(Connection connection
        , PreparedStatement statement) {
      this.connection = connection;
      this.statement = statement;
      try {
        this.connection.setAutoCommit(false);
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Override
    public void write(Writable w) throws IOException {
      ResultSetWritable rs = (ResultSetWritable) w;
      try {
        rs.write(statement);
        statement.addBatch();
      } catch (SQLException e) {
        e.printStackTrace();
      }

    }

    @Override
    public void close(boolean abort) throws IOException {
      // TODO Auto-generated method stub
      try {
        statement.executeBatch();
        connection.commit();
      } catch (SQLException e) {
        try {
          connection.rollback();
        } catch (SQLException ex) {
          LOG.warn(StringUtils.stringifyException(ex));
        }
        throw new IOException(e.getMessage());
      } finally {
        try {
          statement.close();
          connection.close();
        } catch (SQLException ex) {
          throw new IOException(ex.getMessage());
        }
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
  protected String constructQuery(String table, String[] fieldNames) {
    if (fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

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

    return query.toString();
  }

  /** {@inheritDoc} */
  public void checkOutputSpecs(FileSystem filesystem, JobConf job)
      throws IOException {
  }


  /**
   * Initializes the reduce-part of the job with the appropriate output settings
   *
   * @param job
   *          The job
   * @param tableName
   *          The table to insert data into
   * @param fieldNames
   *          The field names in the table. If unknown, supply the appropriate
   *          number of nulls.
   */
  public static void setOutput(JobConf job, String tableName, String... fieldNames) {
    job.setOutputFormat(HiveDBOutputFormat.class);
    job.setReduceSpeculativeExecution(false);

    DBConfiguration dbConf = new DBConfiguration(job);

    dbConf.setOutputTableName(tableName);
    dbConf.setOutputFieldNames(fieldNames);
  }

  @Override
  public RecordWriter getHiveRecordWriter(
      JobConf job, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    // TODO Auto-generated method stub
    DBConfiguration dbConf = new DBConfiguration(job);
    String tableName = dbConf.getOutputTableName();
    String[] fieldNames = dbConf.getOutputFieldNames();
    Connection connection = null;
    PreparedStatement statement = null;

    try {
      connection = new DBManager(dbConf.getConf()).getConnection();
      statement = connection.prepareStatement(constructQuery(tableName, fieldNames));
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return new DBRecordWriter(connection, statement);

  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(FileSystem fileSystem,
      JobConf jobConf, String name, Progressable progressable) throws IOException {
    // TODO Auto-generated method stub
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }
}
