package simplejdbc;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static simplejdbc.TestUtil.assertException;

import com.google.common.collect.ImmutableMap;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

class UpdateBuilderTest {
  private SimpleJdbc simpleJdbc;

  @BeforeEach
  void setup() {
    DataSource dataSource = Mockito.mock(DataSource.class);
    simpleJdbc = Mockito.spy(SimpleJdbc.using(dataSource));
  }

  @Test
  void table_withNullTableName_throws() {
    assertException(() -> simpleJdbc.update().table(null), "table name must not be null");
  }

  @Test
  void table_withBlankTableName_throws() {
    assertException(() -> simpleJdbc.update().table(""), "table name must not be blank");
  }

  @Test
  void table_withSpacesInTableName_throws() {
    assertException(
        () -> simpleJdbc.update().table("table name"),
        "update() does not support table names containing spaces or special characters. Use statement() instead.");
  }

  @Test
  void table_withSpecialCharacterInTableName_throws() {
    assertException(
        () -> simpleJdbc.update().table("table-name"),
        "update() does not support table names containing spaces or special characters. Use statement() instead.");
  }

  @Test
  void set_withNullColumnName_throws() {
    assertException(
        () -> simpleJdbc.update().table("table").set(null, null), "column name must not be null");
  }

  @Test
  void set_withBlankColumnName_throws() {
    assertException(
        () -> simpleJdbc.update().table("table").set("", null), "column name must not be blank");
  }

  @Test
  void set_withSpaceInColumnName_throws() {
    assertException(
        () -> simpleJdbc.update().table("table").set("column name", null),
        "update() does not support column names containing spaces or special characters. Use statement() instead.");
  }

  @Test
  void set_withSpecialCharactersInColumnName_throws() {
    assertException(
        () -> simpleJdbc.update().table("table").set("column-name", null),
        "update() does not support column names containing spaces or special characters. Use statement() instead.");
  }

  @Test
  void set_withCollectionTypedValue_throws() {
    assertException(
        () -> simpleJdbc.update().table("table").set("columnName", ImmutableList.of()),
        "value must not be a collection type");
  }

  @Test
  void where_withNullCondition_throws() {
    assertException(
        () -> simpleJdbc.update().table("table").set("columnName", null).where(null),
        "where() condition must not be null or blank. Did you mean to use executeUnconditionally()?");
  }

  @Test
  void where_withBlankCondition_throws() {
    assertException(
        () -> simpleJdbc.update().table("table").set("columnName", null).where(""),
        "where() condition must not be null or blank. Did you mean to use executeUnconditionally()?");
  }

  @Test
  void bind_withNullName_throws() {
    assertException(
        () ->
            simpleJdbc
                .update()
                .table("table")
                .set("columnName", null)
                .where("foo = :foo")
                .bind(null, 123),
        "bind parameter name must not be null");
  }

  @Test
  void bind_withBlankName_throws() {
    assertException(
        () ->
            simpleJdbc
                .update()
                .table("table")
                .set("columnName", null)
                .where("foo = :foo")
                .bind("", 123),
        "bind parameter name must not be blank");
  }

  @Test
  void bind_withSpacesInName_throws() {
    assertException(
        () ->
            simpleJdbc
                .update()
                .table("table")
                .set("column", 1)
                .where("foo = :foo")
                .bind("param name", 1),
        "bind parameter name must not contain spaces or special characters");
  }

  @Test
  void bind_withSpecialCharactersInName_throws() {
    assertException(
        () ->
            simpleJdbc
                .update()
                .table("table")
                .set("column", 1)
                .where("foo = :foo")
                .bind("param-name", 1),
        "bind parameter name must not contain spaces or special characters");
  }

  @Test
  void executeUnconditionally_makesExpectedSimpleJdbcCoreCall() {
    doReturn(0).when(simpleJdbc).statement(anyString(), anyMap());

    simpleJdbc.update().table("tableName").set("columnName", "value").executeUnconditionally();

    verify(simpleJdbc)
        .statement(
            "update tableName set columnName = :columnName",
            ImmutableMap.of("columnName", "value"));
  }

  @Test
  void execute_makesExpectedSimpleJdbcCoreCall() {
    doReturn(0).when(simpleJdbc).statement(anyString(), anyMap());

    simpleJdbc
        .update()
        .table("tableName")
        .set("columnName", "value")
        .where("foo = :foo")
        .bind("foo", "fooValue")
        .execute();

    verify(simpleJdbc)
        .statement(
            "update tableName set columnName = :columnName where foo = :foo",
            ImmutableMap.of("columnName", "value", "foo", "fooValue"));
  }

  @Test
  void execute_withColumnConflictingParameterName_makesExpectedSimpleJdbcCoreCall() {
    doReturn(0).when(simpleJdbc).statement(anyString(), anyMap());

    simpleJdbc
        .update()
        .table("tableName")
        .set("columnName", "columnValue")
        .where("foo = :columnName")
        .bind("columnName", "bindValue")
        .execute();

    verify(simpleJdbc)
        .statement(
            "update tableName set columnName = :_columnName where foo = :columnName",
            ImmutableMap.of("_columnName", "columnValue", "columnName", "bindValue"));
  }

  @Test
  void execute_withUnboundParameter_throws() {
    assertException(
        () ->
            simpleJdbc
                .update()
                .table("tableName")
                .set("columnName", "columnValue")
                .where("foo = :foo")
                .execute(),
        "no binding provided for parameter :foo");
  }

  @Test
  void execute_withUnboundParameterMatchingColumnName_throws() {
    assertException(
        () ->
            simpleJdbc
                .update()
                .table("tableName")
                .set("columnName", "columnValue")
                .where("foo = :columnName")
                .execute(),
        "no binding provided for parameter :columnName");
  }
}
