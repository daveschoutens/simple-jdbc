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
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import simplejdbc.SimpleJdbc.QueryResultExtractor;

abstract class SimpleJdbcTest {

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
            () -> getSubject().query("sql", ImmutableMap.of(), qr -> null));
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  void update_SQLException_wrappedInCustomException() throws SQLException {
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("test"));
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class, () -> getSubject().statement("sql", ImmutableMap.of()));
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  void batchedUpdate_SQLException_wrappedInCustomException() throws SQLException {
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("test"));
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class,
            () -> getSubject().batchStatement("sql", ImmutableList.of(ImmutableMap.of())));
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  void batchedUpdate_withEmptyBatch_throws() {
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class, () -> getSubject().batchStatement("sql", ImmutableList.of()));
    assertThat(ex).hasMessageThat().contains("Empty batch");
  }

  @Test
  void batchedUpdate_withInvalidBatch_throws() {
    SimpleJdbcException ex =
        assertThrows(
            SimpleJdbcException.class,
            () ->
                getSubject()
                    .batchStatement(
                        "select :foo",
                        ImmutableList.of(
                            ImmutableMap.of("foo", ImmutableList.of(1, 2)),
                            ImmutableMap.of("foo", ImmutableList.of(1, 2, 3)))));
    assertThat(ex).hasMessageThat().contains("Invalid batch");
  }

  @Test
  void select_closesResources() throws SQLException {
    getSubject().query("select 1", ImmutableMap.of(), qr -> null);

    verify(resultSet).close();
    verify(preparedStatement).close();
  }

  @Test
  void update_closesResources() throws SQLException {
    getSubject().statement("select 1", ImmutableMap.of());

    verify(preparedStatement).close();
  }

  @Test
  void batchedUpdate_closesResources() throws SQLException {
    getSubject().batchStatement("select 1", ImmutableList.of(ImmutableMap.of()));

    verify(preparedStatement).close();
  }

  @Test
  void select_appliesParametersCorrectly() throws SQLException {
    getSubject()
        .query(":foo :bar :baz", ImmutableMap.of("baz", 123, "bar", 456, "foo", 789), qr -> null);

    verify(preparedStatement).setInt(1, 789);
    verify(preparedStatement).setInt(2, 456);
    verify(preparedStatement).setInt(3, 123);
    verify(preparedStatement).executeQuery();
  }

  @Test
  void update_appliesParametersCorrectly() throws SQLException {
    getSubject().statement(":foo :bar :baz", ImmutableMap.of("baz", 123, "bar", 456, "foo", 789));

    verify(preparedStatement).setInt(1, 789);
    verify(preparedStatement).setInt(2, 456);
    verify(preparedStatement).setInt(3, 123);
    verify(preparedStatement).executeUpdate();
  }

  @Test
  void batchedUpdate_appliesParametersCorrectly() throws SQLException {
    getSubject()
        .batchStatement(
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
            .query(
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
                    .query(
                        "select 1 as foo",
                        ImmutableMap.of(),
                        qr -> qr.toResultSet().getInt("foo")));
    assertThat(ex).hasCauseThat().hasMessageThat().isEqualTo(errorMessage);
  }

  @Nested
  @SuppressWarnings("unchecked")
  class DSLTests {
    private SimpleJdbc subject;

    @BeforeEach
    void setup() {
      subject = Mockito.spy(getSubject());
    }

    @Nested
    class QueryBuilderTest {
      @Test
      void bindAll_null_throws() {
        assertThrows(NullPointerException.class, () -> subject.query("whatever").bindAll(null));
      }

      @Test
      void select_withNullExtractor_throws() {
        assertThrows(NullPointerException.class, () -> subject.query("whatever").select(null));
      }

      @Test
      void bindAll_test() {
        String sql = "some sql with :bind :variables";
        ImmutableMap<String, Integer> bindings = ImmutableMap.of("bind", 123, "variables", 456);
        QueryResultExtractor<Object> queryResultExtractor = qr -> null;
        subject.query(sql).bindAll(bindings).select(queryResultExtractor);

        assertQueryExecution(sql, bindings, queryResultExtractor);
      }

      @Test
      void bind_test() {
        String sql = "some sql with :bind :variables";
        QueryResultExtractor<Object> queryResultExtractor = qr -> null;
        subject.query(sql).bind("bind", 123).bind("variables", 456).select(queryResultExtractor);

        assertQueryExecution(
            sql, ImmutableMap.of("bind", 123, "variables", 456), queryResultExtractor);
      }

      @Test
      private void assertQueryExecution(
          String sql, Map<String, ?> bindings, QueryResultExtractor<?> queryResultExtractor) {
        ArgumentCaptor<String> sqlArg = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, ?>> bindingsArg = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<QueryResultExtractor<?>> qrArg =
            ArgumentCaptor.forClass(QueryResultExtractor.class);
        verify(subject).query(sqlArg.capture(), bindingsArg.capture(), qrArg.capture());

        assertThat(sqlArg.getValue()).isEqualTo(sql);
        assertThat(bindingsArg.getValue()).isEqualTo(bindings);
        assertThat(qrArg.getValue()).isEqualTo(queryResultExtractor);
      }
    }

    @Nested
    class StatementBuilderTest {
      @Test
      void bindAll_null_throws() {
        assertThrows(NullPointerException.class, () -> subject.statement("whatever").bindAll(null));
      }

      @Test
      void bindAll_test() {
        String sql = "some sql with :bind :variables";
        ImmutableMap<String, Integer> bindings = ImmutableMap.of("bind", 123, "variables", 456);
        subject.statement(sql).bindAll(bindings).execute();

        assertUpdateExecution(sql, bindings);
      }

      @Test
      void bind_test() {
        String sql = "some sql with :bind :variables";
        subject.statement(sql).bind("bind", 123).bind("variables", 456).execute();

        assertUpdateExecution(sql, ImmutableMap.of("bind", 123, "variables", 456));
      }

      private void assertUpdateExecution(String sql, Map<String, ?> bindings) {
        ArgumentCaptor<String> sqlArg = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, ?>> bindingsArg = ArgumentCaptor.forClass(Map.class);
        verify(subject).statement(sqlArg.capture(), bindingsArg.capture());

        assertThat(sqlArg.getValue()).isEqualTo(sql);
        assertThat(bindingsArg.getValue()).isEqualTo(bindings);
      }
    }

    @Nested
    class BatchedStatementBuilderTest {

      @Test
      void batchAdd_null_throws() {
        assertThrows(
            NullPointerException.class, () -> subject.batchStatement("whatever").batchAdd(null));
      }

      @Test
      void batchAddAll_null_throws() {
        assertThrows(
            NullPointerException.class, () -> subject.batchStatement("whatever").batchAddAll(null));
      }

      @Test
      void piecemeal_test() {
        String sql = "some sql with :bind :variables";
        subject
            .batchStatement(sql)
            .bind("bind", 123)
            .bind("variables", 456)
            .addBatch()
            .bind("bind", 321)
            .bind("variables", 654)
            .addBatch()
            .executeBatch();

        assertBatchExecution(
            sql,
            ImmutableList.of(
                ImmutableMap.of("bind", 123, "variables", 456),
                ImmutableMap.of("bind", 321, "variables", 654)));
      }

      @Test
      void batchAdd_test() {
        String sql = "some sql with :bind :variables";
        ImmutableMap<String, Integer> binding = ImmutableMap.of("bind", 321, "variables", 654);
        subject
            .batchStatement(sql)
            .batchAdd(ImmutableMap.of("bind", 123, "variables", 456))
            .batchAdd(binding)
            .executeBatch();

        assertBatchExecution(
            sql,
            ImmutableList.of(
                ImmutableMap.of("bind", 123, "variables", 456),
                ImmutableMap.of("bind", 321, "variables", 654)));
      }

      @Test
      void batchAddAll_test() {
        String sql = "some sql with :bind :variables";
        subject
            .batchStatement(sql)
            .batchAddAll(
                ImmutableList.of(
                    ImmutableMap.of("bind", 123, "variables", 456),
                    ImmutableMap.of("bind", 321, "variables", 654)))
            .executeBatch();

        assertBatchExecution(
            sql,
            ImmutableList.of(
                ImmutableMap.of("bind", 123, "variables", 456),
                ImmutableMap.of("bind", 321, "variables", 654)));
      }

      @Test
      void mixed_test() {
        String sql = "some sql with :bind :variables";
        subject
            .batchStatement(sql)
            .batchAddAll(ImmutableList.of(ImmutableMap.of("bind", 123, "variables", 456)))
            .batchAdd(ImmutableMap.of("bind", 321, "variables", 654))
            .bind("bind", 789)
            .bind("variables", 987)
            .addBatch()
            .executeBatch();

        assertBatchExecution(
            sql,
            ImmutableList.of(
                ImmutableMap.of("bind", 123, "variables", 456),
                ImmutableMap.of("bind", 321, "variables", 654),
                ImmutableMap.of("bind", 789, "variables", 987)));
      }

      private void assertBatchExecution(
          String sql, ImmutableList<ImmutableMap<String, Integer>> batchedBindings) {
        ArgumentCaptor<String> sqlArg = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List<Map<String, ?>>> bindingsArg = ArgumentCaptor.forClass(List.class);
        verify(subject).batchStatement(sqlArg.capture(), bindingsArg.capture());

        assertThat(sqlArg.getValue()).isEqualTo(sql);
        assertThat(bindingsArg.getValue()).isEqualTo(batchedBindings);
      }
    }
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
      subject.query("some query", ImmutableMap.of(), qr -> null);
      verify(connection, times(0)).close();
    }

    @Test
    void update_canReuseConnection() throws SQLException {
      subject.statement("some query", ImmutableMap.of());
      verify(connection, times(0)).close();
    }

    @Test
    void batchedUpdate_canReuseConnection() throws SQLException {
      subject.batchStatement("some query", ImmutableList.of(ImmutableMap.of()));
      verify(connection, times(0)).close();
    }
  }

  @Nested
  class TransactionalTests {
    @BeforeEach
    void setup() throws SQLException {
      when(connection.getAutoCommit()).thenReturn(true);
    }

    @Test
    void doTransactionally_commitsTransaction_andResetsAutoCommit() throws SQLException {
      getSubject().doTransactionally(jdbc -> {});

      verify(connection).setAutoCommit(false);
      verify(connection).commit();
      verify(connection).setAutoCommit(true);
    }

    @Test
    void doTransactionally_rollbackOnException_andResetsAutoCommit() throws SQLException {
      assertThrows(
          RuntimeException.class,
          () ->
              getSubject()
                  .doTransactionally(
                      jdbc -> {
                        throw new RuntimeException("test");
                      }));

      verify(connection).setAutoCommit(false);
      verify(connection).rollback();
      verify(connection).setAutoCommit(true);
    }

    @Test
    void doTransactionally_whenNonSQLExceptionThrown_emitsUnwrapped() {
      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () ->
                  getSubject()
                      .doTransactionally(
                          jdbc -> {
                            throw new RuntimeException("test");
                          }));
      assertThat(ex).hasCauseThat().isNull();
      assertThat(ex).hasMessageThat().isEqualTo("test");
    }

    @Test
    void doTransactionally_whenSQLExceptionThrown_emitsWrapped() {
      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () ->
                  getSubject()
                      .doTransactionally(
                          jdbc -> {
                            throw new SQLException("test");
                          }));
      assertThat(ex).isNotInstanceOf(SQLException.class);
      assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
      assertThat(ex).hasMessageThat().contains("test");
    }

    @Test
    void getTransactionally_commitsTransaction_andResetsAutoCommit() throws SQLException {
      String result = getSubject().getTransactionally(jdbc -> "result");

      assertThat(result).isEqualTo("result");
      verify(connection).setAutoCommit(false);
      verify(connection).commit();
      verify(connection).setAutoCommit(true);
    }

    @Test
    void getTransactionally_rollbackOnException_andResetsAutoCommit() throws SQLException {
      assertThrows(
          RuntimeException.class,
          () ->
              getSubject()
                  .getTransactionally(
                      jdbc -> {
                        throw new RuntimeException("test");
                      }));

      verify(connection).setAutoCommit(false);
      verify(connection).rollback();
      verify(connection).setAutoCommit(true);
    }

    @Test
    void getTransactionally_whenNonSQLExceptionThrown_emitsUnwrapped() {
      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () ->
                  getSubject()
                      .getTransactionally(
                          jdbc -> {
                            throw new RuntimeException("test");
                          }));
      assertThat(ex).hasCauseThat().isNull();
      assertThat(ex).hasMessageThat().isEqualTo("test");
    }

    @Test
    void getTransactionally_whenSQLExceptionThrown_emitsWrapped() {
      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () ->
                  getSubject()
                      .getTransactionally(
                          jdbc -> {
                            throw new SQLException("test");
                          }));
      assertThat(ex).isNotInstanceOf(SQLException.class);
      assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
      assertThat(ex).hasMessageThat().contains("test");
    }
  }
}
