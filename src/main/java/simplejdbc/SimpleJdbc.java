package simplejdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import simplejdbc.InsertBuilder.BatchInsert;

public abstract class SimpleJdbc {

  private final ParameterSetters parameterSetters;
  private final ColumnExtractors columnExtractors;

  public static SimpleJdbc using(DataSource dataSource) {
    return new DataSourceSimpleJdbc(
        dataSource, ParameterSetters.defaults(), ColumnExtractors.defaults());
  }

  public static SimpleJdbc using(Connection connection) {
    return new SingleConnectionSimpleJdbc(
        connection, ParameterSetters.defaults(), ColumnExtractors.defaults());
  }

  public static SimpleJdbc using(
      DataSource dataSource, ParameterSetters parameterSetters, ColumnExtractors columnExtractors) {
    return new DataSourceSimpleJdbc(dataSource, parameterSetters, columnExtractors);
  }

  public static SimpleJdbc using(
      Connection connection, ParameterSetters parameterSetters, ColumnExtractors columnExtractors) {
    return new SingleConnectionSimpleJdbc(connection, parameterSetters, columnExtractors);
  }

  private SimpleJdbc(ParameterSetters parameterSetters, ColumnExtractors columnExtractors) {
    this.parameterSetters = parameterSetters;
    this.columnExtractors = columnExtractors;
  }

  public QueryBuilder query(String sql) {
    return new QueryBuilder(this, sql);
  }

  public StatementBuilder statement(String sql) {
    return new StatementBuilder(this, sql);
  }

  public StatementBatchBuilder batchStatement(String sql) {
    return new StatementBatchBuilder(this, sql);
  }

  public UpdateBuilder.Update update() {
    return UpdateBuilder.get(this);
  }

  public InsertBuilder.Insert insert() {
    return InsertBuilder.get(this);
  }

  public BatchInsert batchInsert() {
    return InsertBuilder.get(this);
  }

  /**
   * @param transactionalFn Consumer&lt;SimpleJdbc&gt;
   * @deprecated Use `transact(fn)` instead
   */
  @Deprecated
  public void transactionally(JdbcConsumer transactionalFn) {
    transact(transactionalFn);
  }

  /**
   * @param isolationLevel TransactionIsolationLevel
   * @param transactionalFn Consumer&lt;SimpleJdbc&gt;
   * @deprecated Use `transact(isolationLevel, fn)` instead
   */
  @Deprecated
  public void transactionally(
      TransactionIsolationLevel isolationLevel, JdbcConsumer transactionalFn) {
    transact(isolationLevel, transactionalFn);
  }

  /**
   * Executes the operation given by `transactionalFn` in a DB transaction, triggering a ROLLBACK
   * if the operation throws an exception, or a COMMIT if successful
   *
   * @param transactionalFn Consumer&lt;SimpleJdbc&gt;
   */
  public void transact(JdbcConsumer transactionalFn) {
    transactionally(TransactionIsolationLevel.getDefault(), transactionalFn);
  }

  /**
   * Executes the operation given by `transactionalFn` in a DB transaction, triggering a ROLLBACK
   * if the operation throws an exception, or a COMMIT if successful
   *
   * <p>Uses the given isolation level
   *
   * @param isolationLevel TransactionIsolationLevel
   * @param transactionalFn Consumer&lt;SimpleJdbc&gt;
   */
  public void transact(TransactionIsolationLevel isolationLevel, JdbcConsumer transactionalFn) {
    transactionally(
        isolationLevel,
        simpleJdbc -> {
          transactionalFn.accept(simpleJdbc);
          return null;
        });
  }

  /**
   * @param <T> type of value returned by invoking `transactionalFn`
   * @param transactionalFn Function&lt;SimpleJdbc, T&gt;
   * @return value returned by invoking `transactionalFn`
   * @deprecated Use `transactAndGet(fn)` instead
   */
  @Deprecated
  public <T> T transactionally(JdbcFunction<T> transactionalFn) {
    return transactAndGet(transactionalFn);
  }

  /**
   * @param <T> type of value returned by invoking `transactionalFn`
   * @param isolationLevel TransactionIsolationLevel
   * @param transactionalFn Function&lt;SimpleJdbc, T&gt;
   * @return value returned by invoking `transactionalFn`
   * @deprecated Use `transactAndGet(isolationLevel, fn)` instead
   */
  @Deprecated
  public <T> T transactionally(
      TransactionIsolationLevel isolationLevel, JdbcFunction<T> transactionalFn) {
    return transactAndGet(isolationLevel, transactionalFn);
  }

  /**
   * Executes the operation given by `transactionalFn` in a DB transaction, returning a result.
   * Triggers a ROLLBACK if the operation throws an exception, or a COMMIT if successful.
   *
   * <p>Uses the given isolation level
   *
   * @param <T> type of value returned by invoking `transactionalFn`
   * @param isolationLevel TransactionIsolationLevel
   * @param transactionalFn Function&lt;SimpleJdbc, T&gt;
   * @return value returned by invoking `transactionalFn`
   * @deprecated Use `transactAndGet(isolationLevel, fn)` instead
   */
  public <T> T transactAndGet(TransactionIsolationLevel isolationLevel, JdbcFunction<T> transactionalFn) {
    return withConnection(conn -> transactionally(conn, isolationLevel, transactionalFn));
  }

  /**
   * Executes the operation given by `transactionalFn` in a DB transaction, returning a result.
   * Triggers a ROLLBACK if the operation throws an exception, or a COMMIT if successful.
   *
   * <p>Uses the given isolation level
   *
   * @param <T> type of value returned by invoking `transactionalFn`
   * @param transactionalFn Function&lt;SimpleJdbc, T&gt;
   * @return value returned by invoking `transactionalFn`
   * @deprecated Use `transactAndGet(isolationLevel, fn)` instead
   */
  public <T> T transactAndGet(JdbcFunction<T> transactionalFn) {
    return transactionally(TransactionIsolationLevel.getDefault(), transactionalFn);
  }

  private <T> T transactionally(
      Connection conn, TransactionIsolationLevel isolationLevel, JdbcFunction<T> transactionalFn) {
    try {
      conn.setTransactionIsolation(isolationLevel.getMagicConstantValue());
      boolean autoCommit = conn.getAutoCommit();
      conn.setAutoCommit(false);
      try {
        T result = transactionalFn.apply(SimpleJdbc.using(conn));
        conn.commit();
        return result;
      } catch (Throwable ex) {
        conn.rollback();
        throw ex;
      } finally {
        conn.setAutoCommit(autoCommit);
      }
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  public enum TransactionIsolationLevel {
    READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    public static TransactionIsolationLevel getDefault() {
      return READ_COMMITTED;
    }

    private final int magicConstantValue;

    TransactionIsolationLevel(int magicConstantValue) {
      this.magicConstantValue = magicConstantValue;
    }

    public int getMagicConstantValue() {
      return magicConstantValue;
    }
  }

  abstract <T> T withConnection(Function<Connection, T> fn);

  abstract <T> T query(String sql, Map<String, ?> bindings, QueryResultExtractor<T> extractor);

  abstract int statement(String sql, Map<String, ?> bindings);

  abstract int[] batchStatement(String sql, List<Map<String, ?>> batchedBindings);

  private static class DataSourceSimpleJdbc extends SimpleJdbc {
    private final DataSource dataSource;

    private DataSourceSimpleJdbc(
        DataSource dataSource,
        ParameterSetters parameterSetters,
        ColumnExtractors columnExtractors) {
      super(parameterSetters, columnExtractors);
      this.dataSource = dataSource;
    }

    @Override
    public <T> T query(String sql, Map<String, ?> bindings, QueryResultExtractor<T> extractor) {
      return withConnection(conn -> query(conn, sql, bindings, extractor));
    }

    @Override
    public int statement(String sql, Map<String, ?> bindings) {
      return withConnection(conn -> statement(conn, sql, bindings));
    }

    @Override
    public int[] batchStatement(String sql, List<Map<String, ?>> batchedBindings) {
      return withConnection(conn -> batchStatement(conn, sql, batchedBindings));
    }

    @Override
    <T> T withConnection(Function<Connection, T> fn) {
      try (Connection conn = dataSource.getConnection()) {
        return fn.apply(conn);
      } catch (SQLException ex) {
        throw new SimpleJdbcException(ex);
      }
    }
  }

  private static class SingleConnectionSimpleJdbc extends SimpleJdbc {
    private final Connection connection;

    private SingleConnectionSimpleJdbc(
        Connection connection,
        ParameterSetters parameterSetters,
        ColumnExtractors columnExtractors) {
      super(parameterSetters, columnExtractors);
      this.connection = connection;
    }

    @Override
    public <T> T query(String sql, Map<String, ?> bindings, QueryResultExtractor<T> extractor) {
      return query(connection, sql, bindings, extractor);
    }

    @Override
    public int statement(String sql, Map<String, ?> bindings) {
      return statement(connection, sql, bindings);
    }

    @Override
    public int[] batchStatement(String sql, List<Map<String, ?>> batchedBindings) {
      return batchStatement(connection, sql, batchedBindings);
    }

    @Override
    <T> T withConnection(Function<Connection, T> fn) {
      return fn.apply(connection);
    }
  }

  <T> T query(
      Connection conn, String sql, Map<String, ?> bindings, QueryResultExtractor<T> extractor) {
    ParameterizedQuery pq = ParameterizedQuery.from(sql, bindings);
    try (PreparedStatement stmt = conn.prepareStatement(pq.getSql())) {
      applyParameters(stmt, pq.getParameters());
      try (ResultSet resultSet = stmt.executeQuery()) {
        return extractor.extract(QueryResult.from(resultSet, columnExtractors));
      }
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  int statement(Connection conn, String sql, Map<String, ?> bindings) {
    ParameterizedQuery pq = ParameterizedQuery.from(sql, bindings);
    try (PreparedStatement stmt = conn.prepareStatement(pq.getSql())) {
      applyParameters(stmt, pq.getParameters());
      return stmt.executeUpdate();
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  int[] batchStatement(Connection conn, String sql, List<Map<String, ?>> bindingsBatch) {
    if (bindingsBatch.isEmpty()) {
      throw new SimpleJdbcException("Empty batch");
    }
    List<ParameterizedQuery> pqs =
        bindingsBatch.stream()
            .map(bindings -> ParameterizedQuery.from(sql, bindings))
            .collect(Collectors.toList());
    validateBatch(pqs);
    String parameterizedQuery = pqs.get(0).getSql();
    try (PreparedStatement stmt = conn.prepareStatement(parameterizedQuery)) {
      for (ParameterizedQuery pq : pqs) {
        applyParameters(stmt, pq.getParameters());
        stmt.addBatch();
      }
      return stmt.executeBatch();
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  private void applyParameters(PreparedStatement stmt, List<Object> parameters)
      throws SQLException {
    int i = 0;
    for (Object param : parameters) {
      parameterSetters.getSetter(param).set(stmt, ++i, param);
    }
  }

  private void validateBatch(List<ParameterizedQuery> parameterizedQueryList) {
    if (parameterizedQueryList.stream().map(ParameterizedQuery::getSql).distinct().count() > 1) {
      throw new SimpleJdbcException(
          "Invalid batch - inconsistent sql parameterization. "
              + "When binding parameters to a collection type in a batch, "
              + "you must ensure each collection has the same length");
    }
  }

  public interface QueryResultExtractor<T> {
    T extract(QueryResult queryResult) throws SQLException;
  }

  public interface QueryRowResultExtractor<T> {
    T extract(QueryRowResult rowResult) throws SQLException;
  }

  public interface JdbcConsumer {
    void accept(SimpleJdbc jdbc) throws SQLException;
  }

  public interface JdbcFunction<T> {
    T apply(SimpleJdbc jdbc) throws SQLException;
  }
}
