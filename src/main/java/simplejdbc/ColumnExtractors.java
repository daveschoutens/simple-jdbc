package simplejdbc;

import static java.lang.String.format;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

class ColumnExtractors {

  public interface ColumnExtractor<T> {
    T extract(ResultSet resultSet, String columnLabel) throws SQLException;
  }

  private final Map<Class<?>, ColumnExtractor<?>> extractorMap = new HashMap<>();

  public static ColumnExtractors defaults() {
    return new ColumnExtractors()
        .registerExtractor(Boolean.class, handleNull(ResultSet::getBoolean))
        .registerExtractor(Short.class, handleNull(ResultSet::getShort))
        .registerExtractor(Integer.class, handleNull(ResultSet::getInt))
        .registerExtractor(Long.class, handleNull(ResultSet::getLong))
        .registerExtractor(Float.class, handleNull(ResultSet::getFloat))
        .registerExtractor(Double.class, handleNull(ResultSet::getDouble))
        .registerExtractor(BigDecimal.class, handleNull(ResultSet::getBigDecimal))
        .registerExtractor(
            Character.class,
            (rs, label) -> {
              String extracted = rs.getString(label);
              if (extracted == null) return null;
              if (extracted.length() == 1) return extracted.charAt(0);
              throw new SimpleJdbcException(
                  "Failed to extract single Character from ResultSet. Extracted String length > 1");
            })
        .registerExtractor(String.class, ResultSet::getString)
        .registerExtractor(
            Instant.class,
            (rs, label) -> {
              Timestamp extracted = rs.getTimestamp(label);
              return extracted == null ? null : extracted.toInstant();
            })
        .registerExtractor(
            LocalDateTime.class,
            (rs, label) -> {
              Timestamp extracted = rs.getTimestamp(label);
              return extracted == null ? null : extracted.toLocalDateTime();
            })
        .registerExtractor(
            LocalDate.class,
            (rs, label) -> {
              Date extracted = rs.getDate(label);
              return extracted == null ? null : extracted.toLocalDate();
            })
        .registerExtractor(
            LocalTime.class,
            (rs, label) -> {
              Time extracted = rs.getTime(label);
              return extracted == null ? null : extracted.toLocalTime();
            });
  }

  private ColumnExtractors() {}

  public <T> ColumnExtractors registerExtractor(Class<T> clazz, ColumnExtractor<T> extractor) {
    extractorMap.put(clazz, extractor);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T> ColumnExtractor<T> getExtractor(Class<T> type) {
    if (!extractorMap.containsKey(type)) {
      throw new UnhandledColumnExtractorTypeException(type);
    }
    return (ColumnExtractor<T>) extractorMap.get(type);
  }

  public static <T> ColumnExtractor<T> handleNull(ColumnExtractor<T> extractor) {
    return (resultSet, columnLabel) -> {
      T value = extractor.extract(resultSet, columnLabel);
      return resultSet.wasNull() ? null : value;
    };
  }

  private static class UnhandledColumnExtractorTypeException extends SimpleJdbcException {

    public UnhandledColumnExtractorTypeException(Class<?> type) {
      super(format("No registered column value extractor for type '%s'", type.getSimpleName()));
    }
  }
}
