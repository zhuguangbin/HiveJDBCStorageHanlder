package org.apache.hadoop.hive.jdbchandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.jdbchandler.JDBCSerDe.ColumnMapping;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class LazyResultSet extends LazyStruct {

  private ResultSetWritable resultSet;
  private List<ColumnMapping> columnsMapping;
  private ArrayList<Object> cachedList;

  public LazyResultSet(LazySimpleStructObjectInspector oi) {
    super(oi);
    // TODO Auto-generated constructor stub
  }

  public void init(ResultSetWritable r, List<ColumnMapping> columnsMapping) {

    this.resultSet = r;
    this.columnsMapping = columnsMapping;
    setParsed(false);
  }


  private void parse() {

    if (getFields() == null) {
      List<? extends StructField> fieldRefs =
          ((StructObjectInspector) getInspector()).getAllStructFieldRefs();
      LazyObject<? extends ObjectInspector>[] fields = new LazyObject<?>[fieldRefs.size()];

      for (int i = 0; i < fields.length; i++) {
        fields[i] = LazyFactory.createLazyObject(
            fieldRefs.get(i).getFieldObjectInspector(),
            false);
      }

      setFields(fields);
      setFieldInited(new boolean[fields.length]);
    }

    Arrays.fill(getFieldInited(), false);
    setParsed(true);
  }


  @Override
  public Object getField(int fieldID) {
    if (!getParsed()) {
      parse();
    }

    return uncheckedGetField(fieldID);
  }

  private Object uncheckedGetField(int fieldID) {

    LazyObject<?>[] fields = getFields();
    boolean[] fieldsInited = getFieldInited();

    if (!fieldsInited[fieldID]) {
      fieldsInited[fieldID] = true;
      ByteArrayRef ref = null;

      byte[] res = resultSet.getResultSetByteArray().getColumns().get(fieldID).getRawData();

      if (res == null) {
        return null;
      } else {
        ref = new ByteArrayRef();
        ref.setData(res);
      }

      if (ref != null) {
        fields[fieldID].init(ref, 0, ref.getData().length);
      }
    }

    return fields[fieldID].getObject();
  }

  /**
   * Get the values of the fields as an ArrayList.
   *
   * @return The values of the fields as an ArrayList.
   */
  @Override
  public ArrayList<Object> getFieldsAsList() {
    if (!getParsed()) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < getFields().length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }


  @Override
  public Object getObject() {
    // TODO Auto-generated method stub
    return this;
  }

}
