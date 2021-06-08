package simplejdbc;

import static com.google.common.truth.Truth.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class SettersAndExtractorsTest {

  @Container
  private final PostgreSQLContainer<?> postgreSQLContainer =
      new PostgreSQLContainer<>(PostgreSQLContainer.IMAGE);

  private DataSource dataSource;
  private ParameterSetters parameterSetters;
  private ColumnExtractors columnExtractors;

  @BeforeEach
  void setUp() {
    dataSource = getDataSource(postgreSQLContainer);
    parameterSetters = ParameterSetters.defaults();
    columnExtractors = ColumnExtractors.defaults();
  }

  @ParameterizedTest
  @MethodSource
  <T> void setAndExtract_valuesShouldMatch(Class<T> type, T value) throws SQLException {
    assertThat(setAndExtract(type, value)).isEqualTo(value);
  }

  private static Stream<Arguments> setAndExtract_valuesShouldMatch() {
    return Stream.of(
        Arguments.of(Boolean.class, true),
        Arguments.of(Boolean.class, false),
        Arguments.of(Boolean.class, null),
        Arguments.of(Short.class, (short) 123),
        Arguments.of(Short.class, null),
        Arguments.of(Integer.class, 123),
        Arguments.of(Integer.class, null),
        Arguments.of(Long.class, 123L),
        Arguments.of(Long.class, null),
        Arguments.of(Float.class, 123.45F),
        Arguments.of(Float.class, null),
        Arguments.of(Double.class, 123.45),
        Arguments.of(Double.class, null),
        Arguments.of(BigDecimal.class, BigDecimal.valueOf(123.45)),
        Arguments.of(BigDecimal.class, null),
        Arguments.of(Character.class, 'c'),
        Arguments.of(Character.class, null),
        Arguments.of(String.class, "string"),
        Arguments.of(String.class, null),
        // When built using JDK 16, this test fails unless we truncate.
        // JDK16 time resolution > Postgres
        Arguments.of(Instant.class, Instant.now().truncatedTo(ChronoUnit.MICROS)),
        Arguments.of(Instant.class, null),
        // When built using JDK 16, this test fails unless we truncate.
        // JDK16 time resolution > Postgres
        Arguments.of(LocalDateTime.class, LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)),
        Arguments.of(LocalDateTime.class, null),
        Arguments.of(LocalDate.class, LocalDate.now()),
        Arguments.of(LocalDate.class, null),
        // java.sql.Time only supports down to the second
        Arguments.of(LocalTime.class, LocalTime.now().truncatedTo(ChronoUnit.SECONDS)),
        Arguments.of(LocalTime.class, null));
  }

  private <T> T setAndExtract(Class<T> type, T value) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement pstmt = conn.prepareStatement("select ? as thing")) {
      parameterSetters.getSetter(value).set(pstmt, 1, value);
      try (ResultSet rs = pstmt.executeQuery()) {
        rs.next();
        return columnExtractors.getExtractor(type).extract(rs, "thing");
      }
    }
  }

  protected DataSource getDataSource(JdbcDatabaseContainer<?> container) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(container.getJdbcUrl());
    hikariConfig.setUsername(container.getUsername());
    hikariConfig.setPassword(container.getPassword());
    hikariConfig.setDriverClassName(container.getDriverClassName());
    return new HikariDataSource(hikariConfig);
  }
}
