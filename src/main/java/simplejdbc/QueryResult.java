package simplejdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.Optional;

public class QueryResult {

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

  public Boolean getBoolean(String columnLabel) {
    return getObject(columnLabel, Boolean.class);
  }

  public Integer getInteger(String columnLabel) {
    return getObject(columnLabel, Integer.class);
  }

  public Short getShort(String columnLabel) {
    return getObject(columnLabel, Short.class);
  }

  public Long getLong(String columnLabel) {
    return getObject(columnLabel, Long.class);
  }

  public Float getFloat(String columnLabel) {
    return getObject(columnLabel, Float.class);
  }

  public Double getDouble(String columnLabel) {
    return getObject(columnLabel, Double.class);
  }

  public BigDecimal getBigDecimal(String columnLabel) {
    return getObject(columnLabel, BigDecimal.class);
  }

  public String getString(String columnLabel) {
    return getObject(columnLabel, String.class);
  }

  public Instant getInstant(String columnLabel) {
    return getObject(columnLabel, Instant.class);
  }

  public LocalDateTime getLocalDateTime(String columnLabel) {
    return getObject(columnLabel, LocalDateTime.class);
  }

  public LocalDate getLocalDate(String columnLabel) {
    return getObject(columnLabel, LocalDate.class);
  }

  public LocalTime getLocalTime(String columnLabel) {
    return getObject(columnLabel, LocalTime.class);
  }

  public <T> T getObject(String columnLabel, Class<T> type) {
    return get(columnLabel, type);
  }

  private <T> T get(String columnLabel, Class<T> type) {
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

  public class OptionalView {

    public <T> Optional<T> getObject(String columnLabel, Class<T> type) {
      return Optional.ofNullable(get(columnLabel, type));
    }

    public Optional<Boolean> getBoolean(String columnLabel) {
      return getObject(columnLabel, Boolean.class);
    }

    public Optional<Integer> getInteger(String columnLabel) {
      return getObject(columnLabel, Integer.class);
    }

    public Optional<Short> getShort(String columnLabel) {
      return getObject(columnLabel, Short.class);
    }

    public Optional<Long> getLong(String columnLabel) {
      return getObject(columnLabel, Long.class);
    }

    public Optional<Float> getFloat(String columnLabel) {
      return getObject(columnLabel, Float.class);
    }

    public Optional<Double> getDouble(String columnLabel) {
      return getObject(columnLabel, Double.class);
    }

    public Optional<BigDecimal> getBigDecimal(String columnLabel) {
      return getObject(columnLabel, BigDecimal.class);
    }

    public Optional<String> getString(String columnLabel) {
      return getObject(columnLabel, String.class);
    }

    public Optional<Instant> getInstant(String columnLabel) {
      return getObject(columnLabel, Instant.class);
    }

    public Optional<LocalDateTime> getLocalDateTime(String columnLabel) {
      return getObject(columnLabel, LocalDateTime.class);
    }

    public Optional<LocalDate> getLocalDate(String columnLabel) {
      return getObject(columnLabel, LocalDate.class);
    }

    public Optional<LocalTime> getLocalTime(String columnLabel) {
      return getObject(columnLabel, LocalTime.class);
    }

    public ResultSet toResultSet() {
      return QueryResult.this.toResultSet();
    }
  }
}
