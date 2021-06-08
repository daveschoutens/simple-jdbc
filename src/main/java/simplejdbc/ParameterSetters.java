package simplejdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

public class ParameterSetters {

  public interface ParameterSetter<T> {
    void set(PreparedStatement preparedStatement, int index, T value) throws SQLException;
  }

  private final Map<Class<?>, ParameterSetter<?>> setterMap = new HashMap<>();

  private ParameterSetters() {}

  public static ParameterSetters defaults() {
    return new ParameterSetters()
        .registerSetter(Boolean.class, PreparedStatement::setBoolean)
        .registerSetter(Short.class, PreparedStatement::setShort)
        .registerSetter(Integer.class, PreparedStatement::setInt)
        .registerSetter(Long.class, PreparedStatement::setLong)
        .registerSetter(Float.class, PreparedStatement::setFloat)
        .registerSetter(Double.class, PreparedStatement::setDouble)
        .registerSetter(BigDecimal.class, PreparedStatement::setBigDecimal)
        .registerSetter(
            Character.class, (ps, index, value) -> ps.setString(index, value.toString()))
        .registerSetter(String.class, PreparedStatement::setString)
        .registerSetter(
            Instant.class, (ps, index, value) -> ps.setTimestamp(index, Timestamp.from(value)))
        .registerSetter(
            LocalDateTime.class,
            (ps, index, value) -> ps.setTimestamp(index, Timestamp.valueOf(value)))
        .registerSetter(
            LocalDate.class, (ps, index, value) -> ps.setDate(index, Date.valueOf(value)))
        .registerSetter(
            LocalTime.class, (ps, index, value) -> ps.setTime(index, Time.valueOf(value)));
  }

  public <T> ParameterSetters registerSetter(Class<T> type, ParameterSetter<T> setter) {
    setterMap.put(type, setter);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T> ParameterSetter<T> getSetter(T value) {
    if (value == null) {
      return (preparedStatement, index, ignored) -> preparedStatement.setNull(index, Types.NULL);
    }
    ParameterSetter<?> setter = setterMap.get(value.getClass());
    if (setter == null) {
      throw new UnsupportedParameterTypeException(value.getClass());
    }
    return (ParameterSetter<T>) setter;
  }

  private static class UnsupportedParameterTypeException extends SimpleJdbcException {
    public UnsupportedParameterTypeException(Class<?> type) {
      super(String.format("No registered parameter setter for type '%s'", type.getSimpleName()));
    }
  }
}
