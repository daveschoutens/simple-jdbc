package simplejdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
}
