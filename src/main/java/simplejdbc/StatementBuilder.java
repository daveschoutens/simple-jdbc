package simplejdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StatementBuilder {

  private final SimpleJdbc jdbc;
  private final String sql;
  private final Map<String, Object> bindings = new HashMap<>();

  public StatementBuilder(SimpleJdbc jdbc, String sql) {
    this.jdbc = jdbc;
    this.sql = sql;
  }

  public <T> StatementBuilder bind(String name, T value) {
    bindings.put(name, value);
    return this;
  }

  public StatementBuilder bindAll(Map<String, ?> bindings) {
    Objects.requireNonNull(bindings, "bindings (map) is must not be null");
    this.bindings.putAll(bindings);
    return this;
  }

  public int execute() {
    return jdbc.statement(sql, bindings);
  }
}
