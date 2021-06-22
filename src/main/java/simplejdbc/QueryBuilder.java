package simplejdbc;

import static simplejdbc.Util.check;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import simplejdbc.SimpleJdbc.QueryResultExtractor;

public class QueryBuilder {

  private final SimpleJdbc jdbc;
  private final String sql;
  private final Map<String, Object> bindings = new HashMap<>();

  QueryBuilder(SimpleJdbc jdbc, String sql) {
    this.jdbc = jdbc;
    this.sql = sql;
  }

  public <T> QueryBuilder bind(String name, T value) {
    bindings.put(name, value);
    return this;
  }

  public QueryBuilder bindAll(Map<String, ?> bindings) {
    Objects.requireNonNull(bindings, "bindings (map) must not be null");
    this.bindings.putAll(bindings);
    return this;
  }

  public <T> T select(QueryResultExtractor<T> extractor) {
    Objects.requireNonNull(extractor, "query result extractor must not be null");
    return jdbc.query(sql, bindings, extractor);
  }

  /**
   * Returns `true` if the query returns any results, otherwise `false`
   *
   * @return `true` if the query returns any results, otherwise `false`
   */
  public boolean selectExists() {
    return select(QueryResult::next);
  }

  /**
   * Convenience method which automatically advances the `ResultSet` and calls the provided callback
   * once per result row. Should be used when a query is expected to return *exactly* one row.
   *
   * <p>Returns the result of calling the given `rowExtractor` on the first row of results. If the
   * query did not return *exactly* one row, an exception is thrown.
   *
   * @param rowExtractor a callback that will map one object per row
   * @return the object returned by the callback (for the *exactly* one row)
   * @throws SimpleJdbcException if query returns zero results
   * @throws SimpleJdbcException if query returns more than one result
   */
  public <T> T selectExactlyOne(QueryResultExtractor<T> rowExtractor) {
    return selectFirst(
            queryResult -> {
              T returnValue = rowExtractor.extract(queryResult);
              check(!queryResult.next(), "expected exactly one result, but got multiple");
              return returnValue;
            })
        .orElseThrow(() -> new SimpleJdbcException("expected exactly one result, but got none"));
  }

  /**
   * Convenience method which automatically advances the `ResultSet` and calls the provided callback
   * once per result row. Should be used when the query is expected to return *at most* one row.
   *
   * <p>Returns an `Optional` containing the mapped object from the `rowExtractor` on the first
   * query result, otherwise `empty()`. If more than one row is returned, an exception is thrown.
   *
   * @param rowExtractor a callback that will map one object per row
   * @return an `Optional` containing the object returned by the callback, or `empty()` if no result
   * @throws SimpleJdbcException if query returns more than one result
   */
  public <T> Optional<T> selectMaybeOne(QueryResultExtractor<T> rowExtractor) {
    return selectFirst(
        queryResult -> {
          T returnValue = rowExtractor.extract(queryResult);
          check(!queryResult.next(), "expected at most one result, but got multiple");
          return returnValue;
        });
  }

  /**
   * Convenience method which automatically advances the `ResultSet` and calls the provided callback
   * once per result row. Should be used when one only cares about the first result, but does not
   * mind if the query yields zero or multiple results.
   *
   * <p>Returns an `Optional` containing the mapped object from the `rowExtractor` on the first
   * query result, otherwise `empty()`. Any additional results are ignored.
   *
   * @param rowExtractor a callback that will map one object per row
   * @return an `Optional` containing the object returned by the callback, or `empty()` if no result
   */
  public <T> Optional<T> selectFirst(QueryResultExtractor<T> rowExtractor) {
    return select(
        queryResult -> {
          if (!queryResult.next()) {
            return Optional.empty();
          }
          T returnValue = rowExtractor.extract(queryResult);
          return Optional.of(returnValue);
        });
  }

  /**
   * Convenience method which automatically advances the `ResultSet` and calls the provided callback
   * once per result row, collecting the results into a List.
   *
   * @param rowExtractor a callback that will map one object per row
   * @return a `List` containing the mapped objects
   */
  public <T> List<T> selectList(QueryResultExtractor<T> rowExtractor) {
    return select(
        queryResult -> {
          List<T> returnValue = new ArrayList<>();
          while (queryResult.next()) {
            returnValue.add(rowExtractor.extract(queryResult));
          }
          return returnValue;
        });
  }
}
