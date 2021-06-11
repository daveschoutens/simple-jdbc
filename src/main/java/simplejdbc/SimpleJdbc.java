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

public abstract class SimpleJdbc {

  private final ParameterSetters parameterSetters;
  private final ColumnExtractors columnExtractors;

  private SimpleJdbc(ParameterSetters parameterSetters, ColumnExtractors columnExtractors) {
    this.parameterSetters = parameterSetters;
    this.columnExtractors = columnExtractors;
  }

  public abstract <T> T select(
      String sql, Map<String, Object> bindings, QueryResultExtractor<T> extractor);

  public abstract int update(String sql, Map<String, Object> bindings);

  public abstract int[] batchedUpdate(String sql, List<Map<String, Object>> batchedBindings);

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
    public <T> T select(
        String sql, Map<String, Object> bindings, QueryResultExtractor<T> extractor) {
      return withNewConnection(conn -> select(conn, sql, bindings, extractor));
    }

    @Override
    public int update(String sql, Map<String, Object> bindings) {
      return withNewConnection(conn -> update(conn, sql, bindings));
    }

    @Override
    public int[] batchedUpdate(String sql, List<Map<String, Object>> batchedBindings) {
      return withNewConnection(conn -> batchedUpdate(conn, sql, batchedBindings));
    }

    private <T> T withNewConnection(Function<Connection, T> fn) {
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
    public <T> T select(
        String sql, Map<String, Object> bindings, QueryResultExtractor<T> extractor) {
      return select(connection, sql, bindings, extractor);
    }

    @Override
    public int update(String sql, Map<String, Object> bindings) {
      return update(connection, sql, bindings);
    }

    @Override
    public int[] batchedUpdate(String sql, List<Map<String, Object>> batchedBindings) {
      return batchedUpdate(connection, sql, batchedBindings);
    }
  }

  protected <T> T select(
      Connection conn,
      String sql,
      Map<String, Object> bindings,
      QueryResultExtractor<T> extractor) {
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

  protected int update(Connection conn, String sql, Map<String, Object> bindings) {
    ParameterizedQuery pq = ParameterizedQuery.from(sql, bindings);
    try (PreparedStatement stmt = conn.prepareStatement(pq.getSql())) {
      applyParameters(stmt, pq.getParameters());
      return stmt.executeUpdate();
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  protected int[] batchedUpdate(
      Connection conn, String sql, List<Map<String, Object>> bindingsBatch) {
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
}
