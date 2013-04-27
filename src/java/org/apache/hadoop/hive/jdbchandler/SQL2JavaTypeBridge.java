package org.apache.hadoop.hive.jdbchandler;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.util.StringUtils;


public class SQL2JavaTypeBridge {

  private static final Log LOG = LogFactory.getLog(SQL2JavaTypeBridge.class);

  private static Map<String, Integer> bridge = new HashMap<String, Integer>();
  private static Set<String> supportedTypes = new HashSet<String>();


  static {

    for (Field field : Types.class.getFields()) {
      try {
        bridge.put(field.getName(), (Integer) field.get(null));
        supportedTypes.add(field.getName());
      } catch (IllegalArgumentException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(StringUtils.stringifyException(e));
      } catch (IllegalAccessException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(StringUtils.stringifyException(e));
      }
    }
  }

  public static void checkSQLType(String typeName) throws SerDeException {
    boolean support = supportedTypes.contains(typeName);
    if (!support) {
      throw new SerDeException("unsupported SQL Type : " + typeName + ". All Supported types : "
          + supportedTypes);
    }
  }

  public static int toSQLTypeCode(String sqlTypeName) throws SerDeException {

    Integer sqlTypeCode = bridge.get(sqlTypeName);

    if (sqlTypeCode != null) {
      return sqlTypeCode;
    } else {
      throw new SerDeException("unsupported SQL Type : " + sqlTypeName + ". All Supported types : "
          + supportedTypes);
    }

  }

  public static String toJavaType(int sqlTypeCode) {

    switch (sqlTypeCode) {
    case Types.BIT:
    case Types.BOOLEAN:
      return "java.lang.Boolean";

    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
      return "java.lang.Integer";

    case Types.BIGINT:
      return "java.lang.Long";

    case Types.DECIMAL:
    case Types.NUMERIC:
      return "java.math.BigDecimal";

    case Types.REAL:
      return "java.lang.Float";

    case Types.FLOAT:
    case Types.DOUBLE:
      return "java.lang.Double";

    case Types.CHAR:
    case Types.VARCHAR:
    case Types.LONGVARCHAR:
    case Types.BINARY:
    case Types.VARBINARY:
    case Types.LONGVARBINARY:
      return "java.lang.String";

    case Types.DATE:
      return "java.sql.Date";

    case Types.TIME:
      return "java.sql.Time";

    case Types.TIMESTAMP:
      return "java.sql.Timestamp";

    default:
      return "java.lang.Object";
    }

  }

}
