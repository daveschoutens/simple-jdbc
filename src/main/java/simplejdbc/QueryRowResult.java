package simplejdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public interface QueryRowResult {

  ResultSet toResultSet();

  OptionalView opt();

  <T> T getObject(String columnLabel, Class<T> type);

  default Boolean getBoolean(String columnLabel) {
    return getObject(columnLabel, Boolean.class);
  }

  default Integer getInteger(String columnLabel) {
    return getObject(columnLabel, Integer.class);
  }

  default Short getShort(String columnLabel) {
    return getObject(columnLabel, Short.class);
  }

  default Long getLong(String columnLabel) {
    return getObject(columnLabel, Long.class);
  }

  default Float getFloat(String columnLabel) {
    return getObject(columnLabel, Float.class);
  }

  default Double getDouble(String columnLabel) {
    return getObject(columnLabel, Double.class);
  }

  default BigDecimal getBigDecimal(String columnLabel) {
    return getObject(columnLabel, BigDecimal.class);
  }

  default String getString(String columnLabel) {
    return getObject(columnLabel, String.class);
  }

  default Instant getInstant(String columnLabel) {
    return getObject(columnLabel, Instant.class);
  }

  default LocalDateTime getLocalDateTime(String columnLabel) {
    return getObject(columnLabel, LocalDateTime.class);
  }

  default LocalDate getLocalDate(String columnLabel) {
    return getObject(columnLabel, LocalDate.class);
  }

  default LocalTime getLocalTime(String columnLabel) {
    return getObject(columnLabel, LocalTime.class);
  }

  interface OptionalView {

    QueryRowResult box();

    default ResultSet toResultSet() {
      return box().toResultSet();
    }

    default <T> Optional<T> getObject(String columnLabel, Class<T> type) {
      return Optional.ofNullable(box().getObject(columnLabel, type));
    }

    default Optional<Boolean> getBoolean(String columnLabel) {
      return getObject(columnLabel, Boolean.class);
    }

    default Optional<Integer> getInteger(String columnLabel) {
      return getObject(columnLabel, Integer.class);
    }

    default Optional<Short> getShort(String columnLabel) {
      return getObject(columnLabel, Short.class);
    }

    default Optional<Long> getLong(String columnLabel) {
      return getObject(columnLabel, Long.class);
    }

    default Optional<Float> getFloat(String columnLabel) {
      return getObject(columnLabel, Float.class);
    }

    default Optional<Double> getDouble(String columnLabel) {
      return getObject(columnLabel, Double.class);
    }

    default Optional<BigDecimal> getBigDecimal(String columnLabel) {
      return getObject(columnLabel, BigDecimal.class);
    }

    default Optional<String> getString(String columnLabel) {
      return getObject(columnLabel, String.class);
    }

    default Optional<Instant> getInstant(String columnLabel) {
      return getObject(columnLabel, Instant.class);
    }

    default Optional<LocalDateTime> getLocalDateTime(String columnLabel) {
      return getObject(columnLabel, LocalDateTime.class);
    }

    default Optional<LocalDate> getLocalDate(String columnLabel) {
      return getObject(columnLabel, LocalDate.class);
    }

    default Optional<LocalTime> getLocalTime(String columnLabel) {
      return getObject(columnLabel, LocalTime.class);
    }
  }
}
