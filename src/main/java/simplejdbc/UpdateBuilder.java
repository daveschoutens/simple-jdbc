package simplejdbc;

import static simplejdbc.Util.COLUMN_NAME_REGEX;
import static simplejdbc.Util.PARAMETER_NAME_REGEX;
import static simplejdbc.Util.TABLE_NAME_REGEX;
import static simplejdbc.Util.check;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Stream;

public class UpdateBuilder {

  static Builder get(SimpleJdbc jdbc) {
    return new Builder(jdbc);
  }

  static class Builder implements Update, UpdateTable, UpdateTableSet, UpdateTableConditions {
    private final SimpleJdbc jdbc;
    private String tableName;
    private final Map<String, Object> columns = new HashMap<>();
    private String sqlConditions;
    private final Map<String, Object> parameters = new HashMap<>();

    private Builder(SimpleJdbc jdbc) {
      this.jdbc = jdbc;
    }

    @Override
    public UpdateTable table(String tableName) {
      check(tableName != null, "table name must not be null");
      check(!tableName.isEmpty(), "table name must not be blank");
      check(
          TABLE_NAME_REGEX.asPredicate().test(tableName),
          "update() does not support table names containing spaces or special characters. Use statement() instead.");
      this.tableName = tableName;
      return this;
    }

    @Override
    public UpdateTableSet set(String columnName, Object value) {
      check(columnName != null, "column name must not be null");
      check(!columnName.isEmpty(), "column name must not be blank");
      check(
          COLUMN_NAME_REGEX.asPredicate().test(columnName),
          "update() does not support column names containing spaces or special characters. Use statement() instead.");
      check(!(value instanceof Collection), "value must not be a collection type");
      columns.put(columnName, value);
      return this;
    }

    @Override
    public UpdateTableConditions where(String sqlConditions) {
      check(!columns.isEmpty(), "must set() at least one column");
      check(
          sqlConditions != null && !sqlConditions.trim().isEmpty(),
          "where() condition must not be null or blank. Did you mean to use executeUnconditionally()?");
      this.sqlConditions = sqlConditions;
      return this;
    }

    @Override
    public UpdateTableConditions bind(String name, Object value) {
      check(name != null, "bind parameter name must not be null");
      check(!name.isEmpty(), "bind parameter name must not be blank");
      check(
          PARAMETER_NAME_REGEX.asPredicate().test(name),
          "bind parameter name must not contain spaces or special characters");
      parameters.put(name, value);
      return this;
    }

    @Override
    public int executeUnconditionally() {
      check(!columns.isEmpty(), "must set() at least one column");
      return execute();
    }

    @Override
    public int execute() {
      validateSqlConditions();
      Map<String, String> columnParameters = disambiguateColumnParameters();
      return jdbc.statement(buildSql(columnParameters), parameters);
    }

    private Map<String, String> disambiguateColumnParameters() {
      Map<String, String> columnParameters = new HashMap<>();
      columns.forEach(
          (columnName, value) -> {
            Stream.iterate(columnName, n -> "_" + n)
                .filter(name -> !parameters.containsKey(name))
                .findFirst()
                .ifPresent(
                    name -> {
                      parameters.put(name, value);
                      columnParameters.put(columnName, name);
                    });
          });
      return columnParameters;
    }

    private void validateSqlConditions() {
      if (sqlConditions != null) {
        // Throw if any parameters in user-specified part of sql are missing bindings
        // This avoids situations where a columnName and paramName match/conflict, but where no
        // explicit binding was provided, from being processed.
        // While it would "work" (column value would be used for both column and other parameter),
        // it is likely to a mistake on the part of the user.
        ParameterizedQuery.from(sqlConditions, parameters);
      }
    }

    private String buildSql(Map<String, String> columnParameters) {
      StringJoiner statement =
          new StringJoiner(
              ", ",
              "update " + tableName + " set ",
              sqlConditions == null ? "" : " where " + sqlConditions);
      columnParameters.forEach(
          (columnName, columnParamName) -> statement.add(columnName + " = :" + columnParamName));
      return statement.toString();
    }
  }

  public interface Update {
    UpdateTable table(String tableName);
  }

  public interface UpdateTable {
    UpdateTableSet set(String columnName, Object value);
  }

  public interface UpdateTableSet extends UpdateTable {
    UpdateTableConditions where(String sqlConditions);

    int executeUnconditionally();
  }

  public interface UpdateTableConditions {
    UpdateTableConditions bind(String name, Object value);

    int execute();
  }
}
