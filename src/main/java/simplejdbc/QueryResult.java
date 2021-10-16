package simplejdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class QueryResult implements QueryRowResult {

  public static QueryResult from(ResultSet resultSet, ColumnExtractors columnExtractors) {
    return new QueryResult(resultSet, columnExtractors);
  }

  private final ResultSet resultSet;
  private final ColumnExtractors columnExtractors;

  private QueryResult(ResultSet resultSet, ColumnExtractors columnExtractors) {
    Objects.requireNonNull(resultSet, "ResultSet not provided");
    Objects.requireNonNull(columnExtractors, "ColumnExtractors not provided");
    this.resultSet = resultSet;
    this.columnExtractors = columnExtractors;
  }

  public boolean next() {
    try {
      return resultSet.next();
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  public <T> T getObject(String columnLabel, Class<T> type) {
    try {
      return columnExtractors.getExtractor(type).extract(resultSet, columnLabel);
    } catch (SQLException ex) {
      throw new SimpleJdbcException(ex);
    }
  }

  public ResultSet toResultSet() {
    return resultSet;
  }

  public OptionalView opt() {
    return new OptionalView();
  }

  public class OptionalView implements QueryRowResult.OptionalView {
    public QueryRowResult box() {
      return QueryResult.this;
    }
  }
}
