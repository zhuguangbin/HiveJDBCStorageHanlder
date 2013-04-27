package org.apache.hadoop.hive.jdbchandler;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.jdbchandler.JDBCSerDe.ColumnMapping;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;


public class JDBCStorageHandler extends DefaultStorageHandler implements HiveMetaHook,
    HiveStoragePredicateHandler {

  public static final Log LOG = LogFactory.getLog(JDBCStorageHandler.class);

  private Configuration conf;
  private DBManager dbManager;

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    // TODO Auto-generated method stub
    this.conf = conf;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    // TODO Auto-generated method stub
    return HiveDBInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    // TODO Auto-generated method stub
    return HiveDBOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    // TODO Auto-generated method stub
    return JDBCSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    // TODO Auto-generated method stub
    return this;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    // TODO Auto-generated method stub
    return super.getAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    // TODO Auto-generated method stub
    configureTableJobProperties(tableDesc, jobProperties);

    Properties tableProperties = tableDesc.getProperties();
    String tableName = tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_NAME);

    jobProperties.put(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    configureTableJobProperties(tableDesc, jobProperties);

    Properties tableProperties = tableDesc.getProperties();
    String tableName = tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_NAME);

    jobProperties.put(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
    jobProperties.put(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, tableProperties.getProperty(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS));

    // set reduce number to 1 and disable specutive execution due to database transaction: when job failed, roll back
    jobProperties.put("mapred.reduce.tasks", "1");
    jobProperties.put("hive.mapred.reduce.tasks.speculative.execution", "false");
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

    // enable driver generate simplified parameter metadata for PreparedStatements
    // when no metadata is available either because the server couldn't support preparing the statement,
    // or server-side prepared statements are disabled
    Properties tableProperties = tableDesc.getProperties();

    jobProperties.put(DBConfiguration.DRIVER_CLASS_PROPERTY,
        tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_DRIVER_CLASS) == null ? "":tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_DRIVER_CLASS));
    jobProperties.put(DBConfiguration.URL_PROPERTY,
        tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_URL) == null ? "":tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_URL));

    jobProperties.put(JDBCSerDe.JDBC_TABLE_NAME,
          tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_NAME) == null ? "":tableProperties.getProperty(JDBCSerDe.JDBC_TABLE_NAME));
    jobProperties.put(JDBCSerDe.JDBC_COLUMNS_MAPPING,
        tableProperties.getProperty(JDBCSerDe.JDBC_COLUMNS_MAPPING) == null ? "":tableProperties.getProperty(JDBCSerDe.JDBC_COLUMNS_MAPPING));

  }

  @Override
  public void preCreateTable(Table tbl) throws MetaException {
    // TODO Auto-generated method stub
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

    if (tbl.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for JDBC Databases.");
    }

    try {

      List<ColumnMapping> columnsMapping = null;

      Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();
      String columnsMappingSpec = serdeParam.get(JDBCSerDe.JDBC_COLUMNS_MAPPING);
      columnsMapping = JDBCSerDe.parseColumnsMapping(columnsMappingSpec);

      conf = initDBConfiguration(tbl);
      String tableName = conf.get(JDBCSerDe.JDBC_TABLE_NAME);
      dbManager = new DBManager(conf);

      if (!dbManager.exists(tableName)) {
        // if it is not an external table then create one
        if (!isExternal) {
          // Create the column descriptors
          dbManager.createTable(tableName, columnsMapping);
        } else {
          // an external table
          throw new MetaException("JDBC table " + tableName +
              " doesn't exist while the table is declared as an external table.");
        }

      } else {
        if (!isExternal) {
          throw new MetaException("Table " + tableName
              + " already exists; use CREATE EXTERNAL TABLE instead to"
              + " register it in Hive.");
        }

      }

    } catch (SerDeException se) {
      LOG.error(StringUtils.stringifyException(se));
      throw new MetaException(StringUtils.stringifyException(se));
    }

  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    // TODO Auto-generated method stub
    boolean isExternal = MetaStoreUtils.isExternalTable(table);

    conf = initDBConfiguration(table);
    String tableName = conf.get(JDBCSerDe.JDBC_TABLE_NAME);
    dbManager = new DBManager(conf);

    if (dbManager.exists(tableName)) {
      // if it is not an external table then drop it
      if (deleteData && !isExternal) {
        dbManager.dropTable(tableName);
      }
    }

  }

  private Configuration initDBConfiguration(Table tbl) {

    Map<String, String> tblParam = tbl.getParameters();
    String driverClassSpec = tblParam.get(JDBCSerDe.JDBC_TABLE_DRIVER_CLASS);
    String urlSpec = tblParam.get(JDBCSerDe.JDBC_TABLE_URL);
    String tableName = tblParam.get(JDBCSerDe.JDBC_TABLE_NAME);
    String columnMappingSpec = tbl.getSd().getSerdeInfo().getParameters()
        .get(JDBCSerDe.JDBC_COLUMNS_MAPPING);

    conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClassSpec);
    conf.set(DBConfiguration.URL_PROPERTY, urlSpec);
    conf.set(JDBCSerDe.JDBC_TABLE_NAME, tableName);
    conf.set(JDBCSerDe.JDBC_COLUMNS_MAPPING, columnMappingSpec);

    return conf;

  }

}
