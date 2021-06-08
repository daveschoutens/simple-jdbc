package simplejdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import simplejdbc.ColumnExtractors.ColumnExtractor;

class QueryResultTest {

  private ColumnExtractors columnExtractors;
  private QueryResult subject;
  private ResultSet resultSet;

  @BeforeEach
  void setup() {
    columnExtractors = ColumnExtractors.defaults();
    resultSet = Mockito.mock(ResultSet.class);
    subject = QueryResult.from(resultSet, columnExtractors);
  }

  @Test
  void withNoResultSet_throws() {
    assertThrows(
        NullPointerException.class, () -> QueryResult.from(null, ColumnExtractors.defaults()));
  }

  @Test
  void escapeHatch_test() {
    assertThat(subject.toResultSet()).isEqualTo(resultSet);
  }

  @ParameterizedTest(name = "getter_dispatch_works - {1}")
  @MethodSource
  @SuppressWarnings("unchecked")
  <T> void getters_dispatchToCorrectColumnExtractor(
      BiFunction<QueryResult, String, T> operation, Class<T> targetType) throws SQLException {
    ColumnExtractor<T> extractor = Mockito.mock(ColumnExtractor.class);
    columnExtractors.registerExtractor(targetType, extractor);

    operation.apply(subject, "columnLabel");

    Mockito.verify(extractor).extract(resultSet, "columnLabel");
  }

  private static Stream<Arguments> getters_dispatchToCorrectColumnExtractor() {
    return Stream.of(
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getBoolean, Boolean.class),
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getShort, Short.class),
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getInteger, Integer.class),
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getLong, Long.class),
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getFloat, Float.class),
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getDouble, Double.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) QueryResult::getBigDecimal, BigDecimal.class),
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getString, String.class),
        Arguments.of((BiFunction<QueryResult, String, ?>) QueryResult::getInstant, Instant.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) QueryResult::getLocalDateTime,
            LocalDateTime.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) QueryResult::getLocalDate, LocalDate.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) QueryResult::getLocalTime, LocalTime.class));
  }

  @ParameterizedTest(name = "optional_getter_dispatch_works - {1}")
  @MethodSource
  @SuppressWarnings("unchecked")
  <T> void optional_getters_dispatchToCorrectColumnExtractor(
      BiFunction<QueryResult, String, T> operation, Class<T> targetType) throws SQLException {
    ColumnExtractor<T> extractor = Mockito.mock(ColumnExtractor.class);
    columnExtractors.registerExtractor(targetType, extractor);

    operation.apply(subject, "columnLabel");

    Mockito.verify(extractor).extract(resultSet, "columnLabel");
  }

  private static Stream<Arguments> optional_getters_dispatchToCorrectColumnExtractor() {
    return Stream.of(
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getBoolean(s), Boolean.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getShort(s), Short.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getInteger(s), Integer.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getLong(s), Long.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getFloat(s), Float.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getDouble(s), Double.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getBigDecimal(s),
            BigDecimal.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getString(s),
            String.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getInstant(s), Instant.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getLocalDateTime(s),
            LocalDateTime.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getLocalDate(s),
            LocalDate.class),
        Arguments.of(
            (BiFunction<QueryResult, String, ?>) (qr, s) -> qr.opt().getLocalTime(s),
            LocalTime.class));
  }
}
