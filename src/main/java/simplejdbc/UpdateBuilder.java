package simplejdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UpdateBuilder {

  private final SimpleJdbc jdbc;
  private final String sql;
  private final Map<String, Object> bindings = new HashMap<>();

  public UpdateBuilder(SimpleJdbc jdbc, String sql) {
    this.jdbc = jdbc;
    this.sql = sql;
  }

  public <T> UpdateBuilder bind(String name, T value) {
    bindings.put(name, value);
    return this;
  }

  public UpdateBuilder bindAll(Map<String, ?> bindings) {
    Objects.requireNonNull(bindings, "bindings (map) is must not be null");
    this.bindings.putAll(bindings);
    return this;
  }

  public int execute() {
    return jdbc.update(sql, bindings);
  }
}
