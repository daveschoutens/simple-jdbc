package simplejdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

abstract class SimpleJdbcTest {

  // TODO: QueryResultSet row-based?
  // TODO: Transactions
  // TODO: QueryBuilder, etc

  protected Connection connection;
  private PreparedStatement preparedStatement;
  private ResultSet resultSet;
  protected ParameterSetters parameterSetters;
  protected ColumnExtractors columnExtractors;

  abstract SimpleJdbc getSubject();

  @BeforeEach
  void base_setup() throws SQLException {
    parameterSetters = ParameterSetters.defaults();
    columnExtractors = ColumnExtractors.defaults();

    connection = Mockito.mock(Connection.class);
    preparedStatement = Mockito.mock(PreparedStatement.class);
    resultSet = Mockito.mock(ResultSet.class);

    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
  }

  @Test
  void select_SQLException_wrappedInCustomException() throws SQLException {
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("test"));
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class,
            () -> getSubject().select("sql", ImmutableMap.of(), qr -> null));
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  void update_SQLException_wrappedInCustomException() throws SQLException {
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("test"));
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class, () -> getSubject().update("sql", ImmutableMap.of()));
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  void batchedUpdate_SQLException_wrappedInCustomException() throws SQLException {
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("test"));
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class,
            () -> getSubject().batchedUpdate("sql", ImmutableList.of(ImmutableMap.of())));
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  void batchedUpdate_withEmptyBatch_throws() {
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class, () -> getSubject().batchedUpdate("sql", ImmutableList.of()));
    assertThat(ex).hasMessageThat().contains("Empty batch");
  }

  @Test
  void batchedUpdate_withInvalidBatch_throws() {
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class,
            () ->
                getSubject()
                    .batchedUpdate(
                        "select :foo",
                        ImmutableList.of(
                            ImmutableMap.of("foo", ImmutableList.of(1, 2)),
                            ImmutableMap.of("foo", ImmutableList.of(1, 2, 3)))));
    assertThat(ex).hasMessageThat().contains("Invalid batch");
  }

  @Test
  void select_closesResources() throws SQLException {
    getSubject().select("select 1", ImmutableMap.of(), qr -> null);

    verify(resultSet).close();
    verify(preparedStatement).close();
  }

  @Test
  void update_closesResources() throws SQLException {
    getSubject().update("select 1", ImmutableMap.of());

    verify(preparedStatement).close();
  }

  @Test
  void batchedUpdate_closesResources() throws SQLException {
    getSubject().batchedUpdate("select 1", ImmutableList.of(ImmutableMap.of()));

    verify(preparedStatement).close();
  }

  @Test
  void select_appliesParametersCorrectly() throws SQLException {
    getSubject()
        .select(":foo :bar :baz", ImmutableMap.of("baz", 123, "bar", 456, "foo", 789), qr -> null);

    verify(preparedStatement).setInt(1, 789);
    verify(preparedStatement).setInt(2, 456);
    verify(preparedStatement).setInt(3, 123);
    verify(preparedStatement).executeQuery();
  }

  @Test
  void update_appliesParametersCorrectly() throws SQLException {
    getSubject().update(":foo :bar :baz", ImmutableMap.of("baz", 123, "bar", 456, "foo", 789));

    verify(preparedStatement).setInt(1, 789);
    verify(preparedStatement).setInt(2, 456);
    verify(preparedStatement).setInt(3, 123);
    verify(preparedStatement).executeUpdate();
  }

  @Test
  void batchedUpdate_appliesParametersCorrectly() throws SQLException {
    getSubject()
        .batchedUpdate(
            ":foo :bar :baz",
            ImmutableList.of(
                ImmutableMap.of("baz", 123, "bar", 456, "foo", 789),
                ImmutableMap.of("baz", 321, "bar", 654, "foo", 987)));

    verify(preparedStatement).setInt(1, 789);
    verify(preparedStatement).setInt(2, 456);
    verify(preparedStatement).setInt(3, 123);
    verify(preparedStatement).setInt(1, 987);
    verify(preparedStatement).setInt(2, 654);
    verify(preparedStatement).setInt(3, 321);
    verify(preparedStatement, times(2)).addBatch();
    verify(preparedStatement).executeBatch();
  }

  @Test
  void select_queryResultExtractor_hasAccessToProvidedColumnExtractors() {
    columnExtractors.registerExtractor(Integer.class, (resultSet, columnLabel) -> 12345);

    int returned =
        getSubject()
            .select(
                "select 1", ImmutableMap.of(), queryResult -> queryResult.getInteger("whatever"));

    assertThat(returned).isEqualTo(12345);
  }

  @Test
  void select_queryResultExtractor_wrapsSQLExceptions_andDoesNotForceChecked() throws SQLException {
    String errorMessage = "some error";
    when(resultSet.getInt("foo")).thenThrow(new SQLException(errorMessage));

    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class,
            () ->
                getSubject()
                    .select(
                        "select 1 as foo",
                        ImmutableMap.of(),
                        qr -> qr.toResultSet().getInt("foo")));
    assertThat(ex).hasCauseThat().hasMessageThat().isEqualTo(errorMessage);
  }

  public static class DataSourceSimpleJdbcTest extends SimpleJdbcTest {
    private SimpleJdbc subject;

    @Override
    SimpleJdbc getSubject() {
      return subject;
    }

    @BeforeEach
    void setup() throws SQLException {
      DataSource dataSource = Mockito.mock(DataSource.class);
      when(dataSource.getConnection()).thenReturn(connection);

      subject = SimpleJdbc.using(dataSource, parameterSetters, columnExtractors);
    }

    @Test
    @Override
    void batchedUpdate_closesResources() throws SQLException {
      super.batchedUpdate_closesResources();
      verify(connection).close();
    }

    @Test
    @Override
    void update_closesResources() throws SQLException {
      super.update_closesResources();
      verify(connection).close();
    }

    @Test
    @Override
    void select_closesResources() throws SQLException {
      super.select_closesResources();
      verify(connection).close();
    }
  }

  public static class SingleConnectionSimpleJdbcTest extends SimpleJdbcTest {
    private SimpleJdbc subject;

    @Override
    SimpleJdbc getSubject() {
      return subject;
    }

    @BeforeEach
    void setup() {
      subject = SimpleJdbc.using(connection, parameterSetters, columnExtractors);
    }

    @Test
    void select_canReuseConnection() throws SQLException {
      subject.select("some query", ImmutableMap.of(), qr -> null);
      verify(connection, times(0)).close();
    }

    @Test
    void update_canReuseConnection() throws SQLException {
      subject.update("some query", ImmutableMap.of());
      verify(connection, times(0)).close();
    }

    @Test
    void batchedUpdate_canReuseConnection() throws SQLException {
      subject.batchedUpdate("some query", ImmutableList.of(ImmutableMap.of()));
      verify(connection, times(0)).close();
    }
  }
}
