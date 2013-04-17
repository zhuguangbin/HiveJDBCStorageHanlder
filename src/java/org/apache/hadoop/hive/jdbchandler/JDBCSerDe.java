package org.apache.hadoop.hive.jdbchandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Writable;

public class JDBCSerDe implements SerDe {

  public static final String JDBC_TABLE_NAME = "hive.jdbc.table.name";
  public static final String JDBC_COLUMNS_MAPPING = "hive.jdbc.table.columns.mapping";
  public static final String JDBC_TABLE_DRIVER_CLASS = "hive.jdbc.table.driver.class";
  public static final String JDBC_TABLE_URL = "hive.jdbc.table.url";


  public static final Log LOG = LogFactory.getLog(JDBCSerDe.class);

  private String jdbcColumnsMapping;
  private List<ColumnMapping> columnsMapping;
  private SerDeParameters serdeParams;

  private LazyResultSet cachedLazyResultSet;
  private ObjectInspector cachedObjectInspector;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    // TODO Auto-generated method stub

    initJDBCSerDeParameters(conf, tbl, getClass().getName());

    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        serdeParams.getColumnNames(),
        serdeParams.getColumnTypes(),
        serdeParams.getSeparators(),
        serdeParams.getNullSequence(),
        serdeParams.isLastColumnTakesRest(),
        serdeParams.isEscaped(),
        serdeParams.getEscapeChar());

    cachedLazyResultSet = new LazyResultSet((LazySimpleStructObjectInspector) cachedObjectInspector);

    if (LOG.isDebugEnabled()) {
      LOG.debug("JDBCSerDe initialized with : columnNames = "
          + serdeParams.getColumnNames()
          + " columnTypes = "
          + serdeParams.getColumnTypes()
          + " jdbcColumnMapping = "
          + jdbcColumnsMapping);
    }

  }

  private void initJDBCSerDeParameters(Configuration conf, Properties tbl, String serdeName)
      throws SerDeException {

    jdbcColumnsMapping = tbl.getProperty(JDBCSerDe.JDBC_COLUMNS_MAPPING);
    columnsMapping = parseColumnsMapping(jdbcColumnsMapping);

    serdeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, serdeName);

  }

  @Override
  public Object deserialize(Writable result) throws SerDeException {
    // TODO Auto-generated method stub
    if (!(result instanceof ResultSetWritable)) {
      throw new SerDeException(getClass().getName() + ": expects ResultSetWritable!");
    }

    cachedLazyResultSet.init((ResultSetWritable) result, columnsMapping);

    return cachedLazyResultSet;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    // TODO Auto-generated method stub
    return cachedObjectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    // TODO Auto-generated method stub
    return ResultSetWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    // TODO Auto-generated method stub
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields =
        (serdeParams.getRowTypeInfo() != null &&
        ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldNames().size() > 0) ?
            ((StructObjectInspector) getObjectInspector()).getAllStructFieldRefs()
            : null;

    ResultSetByteArray rsByteArray = new ResultSetByteArray();
    //TODO:


    ResultSetWritable rs = new ResultSetWritable();
    return rs;
  }

  public static List<ColumnMapping> parseColumnsMapping(String columnsMappingSpec)
      throws SerDeException {

    List<ColumnMapping> result = new ArrayList<ColumnMapping>();

    if (columnsMappingSpec == null) {
      throw new SerDeException("Error: "+JDBC_COLUMNS_MAPPING+" missing for this JDBC table.");

    } else {
      try {
        // example: 'hive.jdbc.columns.mapping'='col1:int(10)#col1comment,col2:varchar(255)#col2comment'
        String[] columnSpecs = columnsMappingSpec.split(",");
        ColumnMapping columnMapping = null;

        for (int i = 0; i < columnSpecs.length; i++) {
          columnMapping = new ColumnMapping();
          String[] colInfo = columnSpecs[i].split("#");

          columnMapping.columnName = colInfo[0].split(":")[0];
          columnMapping.columnType = colInfo[0].split(":")[1];
          columnMapping.columnComments = colInfo[1];

          result.add(columnMapping);
        }
      } catch (IndexOutOfBoundsException e) {
        throw new SerDeException(
            "Error: jdbc.columns.mapping format error for this JDBC table. example: 'jdbc.columns.mapping'='col1:int(10)#col1comment,col2:varchar(255)#col2comment'");

      }
    }
    return result;

  }


  static class ColumnMapping {

    String columnName;
    String columnType;
    String columnComments;

    public String getColumnName() {
      return columnName;
    }

    public void setColumnName(String columnName) {
      this.columnName = columnName;
    }

    public String getColumnType() {
      return columnType;
    }

    public void setColumnType(String columnType) {
      this.columnType = columnType;
    }

    public String getColumnComments() {
      return columnComments;
    }

    public void setColumnComments(String columnComments) {
      this.columnComments = columnComments;
    }
  }

}
