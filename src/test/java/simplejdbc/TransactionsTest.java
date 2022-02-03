package simplejdbc;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TransactionsTest extends DatabaseContainerTest {

  private SimpleJdbc jdbc;

  @BeforeEach
  void setup() {
    jdbc = SimpleJdbc.using(getDataSource());
    jdbc.statement("create table if not exists some_table (col_a int, col_b varchar)").execute();
    jdbc.statement("truncate some_table").execute();
  }

  @Test
  void tempTables_work() {
    String result =
        jdbc.transactionally(
            tx -> {
              tx.statement("create temp table foo (id varchar) on commit drop").execute();
              tx.insert().into("foo").set("id", "abc").execute();
              return tx.query("select id from foo").selectExactlyOne(rs -> rs.getString("id"));
            });
    assertThat(result).isEqualTo("abc");
  }

  @Test
  void success_commits() {
    jdbc.insert().into("some_table").set("col_a", 123).set("col_b", "original_value").execute();

    jdbc.transactionally(
        tx -> {
          tx.update()
              .table("some_table")
              .set("col_b", "different_value")
              .where("col_a = :val")
              .bind("val", 123)
              .execute();
        });
    String result =
        jdbc.query("select col_b from some_table where col_a = :val")
            .bind("val", 123)
            .selectExactlyOne(rs -> rs.getString("col_b"));

    assertThat(result).isEqualTo("different_value");
  }

  @Test
  void exception_rollsBack() {
    jdbc.insert().into("some_table").set("col_a", 123).set("col_b", "original_value").execute();

    try {
      jdbc.transactionally(
          tx -> {
            tx.update()
                .table("some_table")
                .set("col_b", "different_value")
                .where("col_a = :val")
                .bind("val", 123)
                .execute();
            if (true) throw new RuntimeException("blah");
          });
    } catch (Exception ignored) {
    }

    String result =
        jdbc.query("select col_b from some_table where col_a = :val")
            .bind("val", 123)
            .selectExactlyOne(rs -> rs.getString("col_b"));

    assertThat(result).isEqualTo("original_value");
  }
}
