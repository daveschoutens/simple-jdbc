package simplejdbc;

import static simplejdbc.Util.COLUMN_NAME_REGEX;
import static simplejdbc.Util.TABLE_NAME_REGEX;
import static simplejdbc.Util.check;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class InsertBuilder {

  static Builder get(SimpleJdbc jdbc) {
    return new Builder(jdbc);
  }

  static class Builder
      implements Insert,
          InsertInto,
          InsertIntoSet,
          InsertBatch,
          InsertBatchInto,
          InsertBatchIntoSet,
          InsertBatchReady {
    private final SimpleJdbc jdbc;
    private String tableName;
    private Map<String, Object> columnValues = new HashMap<>();
    private final List<Map<String, ?>> batch = new ArrayList<>();

    private Builder(SimpleJdbc jdbc) {
      this.jdbc = jdbc;
    }

    @Override
    public Builder into(String tableName) {
      check(tableName != null && !tableName.isEmpty(), "table name is required");
      check(
          TABLE_NAME_REGEX.asPredicate().test(tableName),
          "insert() does not support table names which contain spaces or special characters. Use statement() instead.");
      this.tableName = tableName;
      return this;
    }

    @Override
    public Builder set(String columnName, Object value) {
      check(columnName != null && !columnName.isEmpty(), "column name is required");
      check(
          COLUMN_NAME_REGEX.asPredicate().test(columnName),
          "insert() does not support column names which contain spaces or special characters. Use statement() instead.");
      check(!(value instanceof Collection), "value must not be a collection type");
      columnValues.put(columnName, value);
      return this;
    }

    @Override
    public Builder addBatch() {
      batch.add(columnValues);
      columnValues = new HashMap<>();
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int[] executeBatch() {
      return jdbc.batchStatement(buildSql((Map<String, Object>) batch.get(0)), batch);
    }

    @Override
    public int execute() {
      return jdbc.statement(buildSql(columnValues), columnValues);
    }

    private String buildSql(Map<String, Object> columnValues) {
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
    InsertIntoSet set(String columnName, Object value);
  }

  public interface InsertIntoSet {
    InsertIntoSet set(String columnName, Object value);

    int execute();
  }

  public interface InsertBatch {
    InsertBatchInto into(String tableName);
  }

  public interface InsertBatchInto {
    InsertBatchIntoSet set(String columnName, Object value);
  }

  public interface InsertBatchIntoSet {
    InsertBatchIntoSet set(String columnName, Object value);

    InsertBatchReady addBatch();
  }

  public interface InsertBatchReady {

    InsertBatchIntoSet set(String columnName, Object value);

    int[] executeBatch();
  }
}
