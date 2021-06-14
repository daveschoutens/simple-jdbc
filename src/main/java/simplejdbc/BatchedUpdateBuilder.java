package simplejdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BatchedUpdateBuilder {

  private final SimpleJdbc jdbc;
  private final String sql;
  private final List<Map<String, ?>> batchedBindings = new ArrayList<>();

  public BatchedUpdateBuilder(SimpleJdbc jdbc, String sql) {
    this.jdbc = jdbc;
    this.sql = sql;
  }

  public BatchedUpdateBuilder batchAdd(Map<String, ?> bindings) {
    Objects.requireNonNull(bindings, "bindings (map) must not be null");
    this.batchedBindings.add(bindings);
    return this;
  }

  public BatchedUpdateBuilder batchAddAll(Collection<Map<String, ?>> bindingsBatch) {
    Objects.requireNonNull(batchedBindings, "bindings batch (list) must not be null");
    this.batchedBindings.addAll(bindingsBatch);
    return this;
  }

  public BatchValueBuilder bind(String name, Object value) {
    return new BatchValueBuilder().bind(name, value);
  }

  public class BatchValueBuilder {
    private final Map<String, Object> batchValueBuilder = new HashMap<>();

    public BatchValueBuilder bind(String name, Object value) {
      batchValueBuilder.put(name, value);
      return this;
    }

    public BatchedUpdateBuilder addBatch() {
      return BatchedUpdateBuilder.this.batchAdd(batchValueBuilder);
    }
  }

  public int[] executeBatch() {
    return jdbc.batchedUpdate(sql, batchedBindings);
  }
}
