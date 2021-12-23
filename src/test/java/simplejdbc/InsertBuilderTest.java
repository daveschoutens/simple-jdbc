package simplejdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
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

class InsertBuilderTest {

  private SimpleJdbc simpleJdbc;

  @BeforeEach
  void setup() {
    DataSource dataSource = Mockito.mock(DataSource.class);
    simpleJdbc = Mockito.spy(SimpleJdbc.using(dataSource));
  }

  @Test
  void into_nullTableName_throws() {
    assertException(() -> simpleJdbc.insert().into(""), "table name is required");
  }

  @Test
  void into_blankTableName_throws() {
    assertException(() -> simpleJdbc.insert().into(""), "table name is required");
  }

  @Test
  void into_tableNameWithSpaces_throws() {
    assertException(
        () -> simpleJdbc.insert().into("table name"),
        "insert() does not support table names which contain spaces or special characters. Use statement() instead.");
  }

  @Test
  void into_tableNameWithSpecialCharacter_throws() {
    assertException(
        () -> simpleJdbc.insert().into("table-name"),
        "insert() does not support table names which contain spaces or special characters. Use statement() instead.");
  }

  @Test
  void set_nullColumnName_throws() {
    assertException(
        () -> simpleJdbc.insert().into("tableName").set(null, null), "column name is required");
  }

  @Test
  void set_noColumnName_throws() {
    assertException(
        () -> simpleJdbc.insert().into("tableName").set("", null), "column name is required");
  }

  @Test
  void set_collectionValue_throws() {
    assertException(
        () -> simpleJdbc.insert().into("tableName").set("column", ImmutableList.of()),
        "value must not be a collection type");
  }

  @Test
  void set_columnNameWithSpaces_throws() {
    assertException(
        () -> simpleJdbc.insert().into("tableName").set("column name", null),
        "insert() does not support column names which contain spaces or special characters. Use statement() instead.");
  }

  @Test
  void set_columnNameWithSpecialCharacters_throws() {
    assertException(
        () -> simpleJdbc.insert().into("table").set("column-name", null),
        "insert() does not support column names which contain spaces or special characters. Use statement() instead.");
  }

  @Test
  void insert_resultsInExpectedCoreCall() {
    doReturn(0).when(simpleJdbc).statement(anyString(), anyMap());

    simpleJdbc.insert().into("table").set("column", 123).set("otherColumn", "someValue").execute();

    verify(simpleJdbc)
        .statement(
            "insert into table (column, otherColumn) values (:column, :otherColumn)",
            ImmutableMap.of("column", 123, "otherColumn", "someValue"));
  }

  @Test
  void insert_withFullyQualifiedTableName_resultsInExpectedCoreCall() {
    doReturn(0).when(simpleJdbc).statement(anyString(), anyMap());

    simpleJdbc
        .insert()
        .into("schema_name.table_name")
        .set("column", 123)
        .set("otherColumn", "someValue")
        .execute();

    verify(simpleJdbc)
        .statement(
            "insert into schema_name.table_name (column, otherColumn) values (:column, :otherColumn)",
            ImmutableMap.of("column", 123, "otherColumn", "someValue"));
  }

  @Test
  void insertBatch_resultsInExpectedCoreCall() {
    doReturn(new int[0]).when(simpleJdbc).batchStatement(anyString(), anyList());

    simpleJdbc
        .batchInsert()
        .into("table")
        .set("column", 123)
        .set("otherColumn", "someValue")
        .addBatch()
        .set("column", 456)
        .set("otherColumn", "someOtherValue")
        .addBatch()
        .executeBatch();

    verify(simpleJdbc)
        .batchStatement(
            "insert into table (column, otherColumn) values (:column, :otherColumn)",
            ImmutableList.of(
                ImmutableMap.of("column", 123, "otherColumn", "someValue"),
                ImmutableMap.of("column", 456, "otherColumn", "someOtherValue")));
  }

  @Test
  void insertBatch_withBatchSizeOfZero_ok() {
    assertThat(simpleJdbc.batchInsert().into("table").executeBatch()).isEmpty();
  }
}
