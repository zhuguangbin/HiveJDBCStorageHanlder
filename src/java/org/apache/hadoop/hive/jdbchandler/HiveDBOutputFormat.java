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
import java.sql.DatabaseMetaData;
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
 */
public class HiveDBOutputFormat<K extends Writable, V extends ResultSetWritable> implements
    HiveOutputFormat<K, V>,
    OutputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(HiveDBOutputFormat.class);


  /** {@inheritDoc} */
  public void checkOutputSpecs(FileSystem filesystem, JobConf job)
      throws IOException {
  }


  @Override
  public RecordWriter getHiveRecordWriter(
      JobConf job, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    // TODO Auto-generated method stub
    DBConfiguration dbConf = new DBConfiguration(job);
    String tableName = dbConf.getOutputTableName();
    String[] fieldNames = dbConf.getOutputFieldNames();
    boolean truncate = dbConf.getTruncateBeforeInsert();
    boolean replace = dbConf.getOutputReplace();
    Connection connection = null;

    try {
      connection = new DBManager(dbConf.getConf()).getConnection();
      DatabaseMetaData dbMeta = connection.getMetaData();
      String dbProductName = dbMeta.getDatabaseProductName().toUpperCase();

      // use database product name to determine appropriate record writer.
      if (dbProductName.startsWith("ORACLE")) {
        // use Oracle-specific db writer.
        return new OracleDBRecordWriter(connection, tableName, fieldNames, truncate, replace);
      } else if (dbProductName.startsWith("MYSQL")) {
        // use MySQL-specific db writer.
        return new MySQLDBRecordWriter(connection, tableName, fieldNames, truncate, replace);
      } else {
        // Generic writer.
        return new DBRecordWriter(connection, tableName, fieldNames, truncate, replace);
      }

    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(StringUtils.stringifyException(e));
    }

  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(FileSystem fileSystem,
      JobConf jobConf, String name, Progressable progressable) throws IOException {
    // TODO Auto-generated method stub
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }
}
