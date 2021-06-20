package simplejdbc;

import static simplejdbc.Util.COLUMN_NAME_REGEX;
import static simplejdbc.Util.TABLE_NAME_REGEX;
import static simplejdbc.Util.check;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class InsertBuilder {

  static Insert get(SimpleJdbc jdbc) {
    return new Builder(jdbc);
  }

  static class Builder implements Insert, InsertInto {
    private final SimpleJdbc jdbc;
    private String tableName;
    private final Map<String, Object> columnValues = new HashMap<>();

    private Builder(SimpleJdbc jdbc) {
      this.jdbc = jdbc;
    }

    @Override
    public InsertInto into(String tableName) {
      check(tableName != null && !tableName.isEmpty(), "table name is required");
      check(
          TABLE_NAME_REGEX.asPredicate().test(tableName),
          "insert() does not support table names which contain spaces or special characters. Use statement() instead.");
      this.tableName = tableName;
      return this;
    }

    @Override
    public InsertInto set(String columnName, Object value) {
      check(columnName != null && !columnName.isEmpty(), "column name is required");
      check(
          COLUMN_NAME_REGEX.asPredicate().test(columnName),
          "insert() does not support column names which contain spaces or special characters. Use statement() instead.");
      check(!(value instanceof Collection), "value must not be a collection type");
      columnValues.put(columnName, value);
      return this;
    }

    @Override
    public int execute() {
      return jdbc.statement(buildSql(), columnValues);
    }

    private String buildSql() {
      StringJoiner insertFragment = new StringJoiner(", ", "insert into " + tableName + " (", ")");
      StringJoiner valuesFragment = new StringJoiner(", ", " values (", ")");
      columnValues.forEach(
          (k, v) -> {
            insertFragment.add(k);
            valuesFragment.add(":" + k);
          });
      return "" + insertFragment + valuesFragment;
    }
  }

  public interface Insert {
    InsertInto into(String tableName);
  }

  public interface InsertInto {
    InsertInto set(String columnName, Object value);

    int execute();
  }
}
