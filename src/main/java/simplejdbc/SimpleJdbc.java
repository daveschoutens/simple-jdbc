package simplejdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public class SimpleJdbc {

  private final ParameterSetters parameterSetters;
  private final ColumnExtractors columnExtractors;

  public SimpleJdbc(ParameterSetters parameterSetters, ColumnExtractors columnExtractors) {
    this.parameterSetters = parameterSetters;
    this.columnExtractors = columnExtractors;
  }

  <T> T select(
      DataSource dataSource,
      String sql,
      Map<String, Object> bindings,
      QueryResultExtractor<T> extractor) {
    try (Connection conn = dataSource.getConnection()) {
      return select(conn, sql, bindings, extractor);
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  <T> T select(
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

  int update(DataSource dataSource, String sql, Map<String, Object> bindings) {
    try (Connection conn = dataSource.getConnection()) {
      return update(conn, sql, bindings);
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  int update(Connection conn, String sql, Map<String, Object> bindings) {
    ParameterizedQuery pq = ParameterizedQuery.from(sql, bindings);
    try (PreparedStatement stmt = conn.prepareStatement(pq.getSql())) {
      applyParameters(stmt, pq.getParameters());
      return stmt.executeUpdate();
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  int[] batchedUpdate(DataSource dataSource, String sql, List<Map<String, Object>> bindingsBatch) {
    try (Connection conn = dataSource.getConnection()) {
      return batchedUpdate(conn, sql, bindingsBatch);
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  int[] batchedUpdate(Connection conn, String sql, List<Map<String, Object>> bindingsBatch) {
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
