package org.apache.hadoop.hive.jdbchandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

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

  // used for serializing a field
  private byte[] separators; // the separators array
  private boolean escaped; // whether we need to escape the data when writing out
  private byte escapeChar; // which char to use as the escape char, e.g. '\\'
  private boolean[] needsEscape; // which chars need to be escaped. This array should have size
                                 // of 128. Negative byte values (or byte values >= 128) are
                                 // never escaped.


  private final ByteStream.Output serializeStream = new ByteStream.Output();


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

    if (columnsMapping.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
          serdeParams.getColumnNames().size() +
          " elements while " + JDBC_COLUMNS_MAPPING + " has " +
          columnsMapping.size() + " elements" +
          " (counting the key if implicit)");
    }

    separators = serdeParams.getSeparators();
    escaped = serdeParams.isEscaped();
    escapeChar = serdeParams.getEscapeChar();
    needsEscape = serdeParams.getNeedsEscape();

    // check that the mapping schema is right;
    // column can be only mapped to Primitive type, cannot support List, Map, Struct and Union
    for (int i = 0; i < columnsMapping.size(); i++) {
      ColumnMapping colMap = columnsMapping.get(i);
      TypeInfo typeInfo = serdeParams.getColumnTypes().get(i);
      if (typeInfo.getCategory() != Category.PRIMITIVE) {
        throw new SerDeException(
            serdeName + ": jdbc column '" + colMap.getColumnName()
                + "' should be mapped to primitive type, but is mapped to "
                + typeInfo.getTypeName());
      }
    }

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
    ResultSetWritable rs = new ResultSetWritable();
    ResultSetByteArray rsByteArray = new ResultSetByteArray();
    List<ResultSetByteArray.Column> columns = new ArrayList<ResultSetByteArray.Column>();

    try {
      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        ResultSetByteArray.Column column = rsByteArray.new Column();
        serializeField(i, column, fields, list, declaredFields);
        columns.add(column);
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new SerDeException(StringUtils.stringifyException(e));
    } catch (IllegalArgumentException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new SerDeException(StringUtils.stringifyException(e));
    } catch (IllegalAccessException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new SerDeException(StringUtils.stringifyException(e));
    }
    rsByteArray.setColumnCount(fields.size());
    rsByteArray.setColumns(columns);

    rs.setResultSetByteArray(rsByteArray);

    return rs;
  }

  private byte[] serializeField(int i, ResultSetByteArray.Column column,
      List<? extends StructField> fields,
      List<Object> list,
      List<? extends StructField> declaredFields) throws IOException, SerDeException,
      IllegalArgumentException, IllegalAccessException {

    // column mapping info
    ColumnMapping colMap = columnsMapping.get(i);


    // Get the field objectInspector and the field object.
    ObjectInspector foi = fields.get(i).getFieldObjectInspector();
    Object f = (list == null ? null : list.get(i));

    if (f == null) {
      // a null object, we do not serialize it
      return null;
    }

    // If the field that is passed in is NOT a primitive, and either the
    // field is not declared (no schema was given at initialization), or
    // the field is declared as a primitive in initialization, serialize
    // the data to JSON string. Otherwise serialize the data in the
    // delimited way.
    serializeStream.reset();
    boolean isNotNull;
    if (!foi.getCategory().equals(Category.PRIMITIVE)
        && (declaredFields == null || declaredFields.get(i).getFieldObjectInspector().getCategory()
            .equals(Category.PRIMITIVE))) {

      // we always serialize the String type using the escaped algorithm for LazyString
      isNotNull = serialize(
          SerDeUtils.getJSONString(f, foi),
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          1, false);
    } else {
      // use the serialization option switch to write primitive values as either a variable
      // length UTF8 string or a fixed width bytes if serializing in binary format
      isNotNull = serialize(f, foi, 1, false);
    }
    if (!isNotNull) {
      return null;
    }

    byte[] data = new byte[serializeStream.getCount()];
    System.arraycopy(serializeStream.getData(), 0, data, 0, serializeStream.getCount());

    int typecode = SQL2JavaTypeBridge.toSQLTypeCode(colMap.getColumnType());
    column.setType(typecode);
    column.setDataLength(data.length);
    column.setRawData(data);

    return data;
  }

  /*
   * Serialize the row into a ByteStream.
   *
   * @param obj The object for the current field.
   *
   * @param objInspector The ObjectInspector for the current Object.
   *
   * @param level The current level of separator.
   *
   * @param writeBinary Whether to write a primitive object as an UTF8 variable length string or
   * as a fixed width byte array onto the byte stream.
   *
   * @throws IOException On error in writing to the serialization stream.
   *
   * @return true On serializing a non-null object, otherwise false.
   */
  private boolean serialize(
      Object obj,
      ObjectInspector objInspector,
      int level,
      boolean writeBinary) throws IOException {

    if (objInspector.getCategory() == Category.PRIMITIVE && writeBinary) {
      LazyUtils.writePrimitive(serializeStream, obj, (PrimitiveObjectInspector) objInspector);
      return true;
    } else {
      return serialize(obj, objInspector, level);
    }
  }

  private boolean serialize(
      Object obj,
      ObjectInspector objInspector,
      int level) throws IOException {

    switch (objInspector.getCategory()) {
    case PRIMITIVE: {
      LazyUtils.writePrimitiveUTF8(serializeStream, obj,
          (PrimitiveObjectInspector) objInspector, escaped, escapeChar, needsEscape);
      return true;
    }

    case LIST: {
      char separator = (char) separators[level];
      ListObjectInspector loi = (ListObjectInspector) objInspector;
      List<?> list = loi.getList(obj);
      ObjectInspector eoi = loi.getListElementObjectInspector();
      if (list == null) {
        return false;
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            serializeStream.write(separator);
          }
          serialize(list.get(i), eoi, level + 1);
        }
      }
      return true;
    }

    case MAP: {
      char separator = (char) separators[level];
      char keyValueSeparator = (char) separators[level + 1];
      MapObjectInspector moi = (MapObjectInspector) objInspector;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(obj);
      if (map == null) {
        return false;
      } else {
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (first) {
            first = false;
          } else {
            serializeStream.write(separator);
          }
          serialize(entry.getKey(), koi, level + 2);
          serializeStream.write(keyValueSeparator);
          serialize(entry.getValue(), voi, level + 2);
        }
      }
      return true;
    }

    case STRUCT: {
      char separator = (char) separators[level];
      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<Object> list = soi.getStructFieldsDataAsList(obj);
      if (list == null) {
        return false;
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            serializeStream.write(separator);
          }

          serialize(list.get(i), fields.get(i).getFieldObjectInspector(),
              level + 1);
        }
      }
      return true;
    }
    }

    throw new RuntimeException("Unknown category type: " + objInspector.getCategory());
  }

  public static List<ColumnMapping> parseColumnsMapping(String columnsMappingSpec)
      throws SerDeException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(JDBC_COLUMNS_MAPPING + " = " + columnsMappingSpec);
    }

    List<ColumnMapping> result = new ArrayList<ColumnMapping>();

    if (columnsMappingSpec == null) {
      throw new SerDeException("Error: " + JDBC_COLUMNS_MAPPING + " missing for this JDBC table.");

    } else {
      try {
        // example:
        // 'hive.jdbc.columns.mapping'='col1:int(10)#col1comment,col2:varchar(255)#col2comment'
        String[] columnSpecs = columnsMappingSpec.split(",");
        ColumnMapping columnMapping = null;

        for (int i = 0; i < columnSpecs.length; i++) {
          columnMapping = new ColumnMapping();
          String[] colInfo = columnSpecs[i].split("#");

          columnMapping.setColumnName(colInfo[0].split(":")[0]); // col1
          String typeInfo = colInfo[0].split(":")[1]; // int(10)

          Pattern withLengthPtn = Pattern.compile("\\w+\\(\\d+\\)");
          Pattern withoutLengthPtn = Pattern.compile("\\w+");
          if (withLengthPtn.matcher(typeInfo).matches()) {
            String typeName = typeInfo.substring(0, typeInfo.indexOf("(")).toUpperCase();
            SQL2JavaTypeBridge.checkSQLType(typeName);
            int colLength = Integer.parseInt(typeInfo.substring(
                typeInfo.indexOf("(") + 1, typeInfo.indexOf(")")));
            columnMapping.setColumnType(typeName);
            columnMapping.setColumnLength(colLength);

          } else if (withoutLengthPtn.matcher(typeInfo).matches()) {
            String typeName = typeInfo.toUpperCase();
            SQL2JavaTypeBridge.checkSQLType(typeName);
            columnMapping.setColumnType(typeName);
         // default max column length 255
            columnMapping.setColumnLength(255);
          } else {
            throw new SerDeException(
                "Error: jdbc.columns.mapping format error for this JDBC table. example: 'jdbc.columns.mapping'='col1:int(10)#col1comment,col2:varchar(255)#col2comment'. ");
          }

          columnMapping.setColumnComments(colInfo[1]); // col1comment

          result.add(columnMapping);
        }
      } catch (IndexOutOfBoundsException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new SerDeException(
            "Error: jdbc.columns.mapping format error for this JDBC table. example: 'jdbc.columns.mapping'='col1:int(10)#col1comment,col2:varchar(255)#col2comment'. ");

      }
    }
    return result;

  }

  // column description in jdbc table

  static class ColumnMapping {

    String columnName;
    String columnType;
    int columnLength;
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

    public int getColumnLength() {
      return columnLength;
    }

    public void setColumnLength(int columnLength) {
      this.columnLength = columnLength;
    }

    public String getColumnComments() {
      return columnComments;
    }

    public void setColumnComments(String columnComments) {
      this.columnComments = columnComments;
    }
  }

}
