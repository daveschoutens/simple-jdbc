package simplejdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import simplejdbc.ParameterizedQuery.MissingParameterBindingException;

class ParameterizedQueryTest {

  @Test
  void withNullQuery_throws() {
    assertThrows(
        NullPointerException.class, () -> ParameterizedQuery.from(null, ImmutableMap.of()));
  }

  @Test
  void withNullBindings_throws() {
    assertThrows(NullPointerException.class, () -> ParameterizedQuery.from("", null));
  }

  @Test
  void withNoParameters_returnsUnmodifiedQuery() {
    String query = "select * from foo";

    ParameterizedQuery result = ParameterizedQuery.from(query, ImmutableMap.of());

    assertThat(result.getSql()).isEqualTo(query);
  }

  @Test
  void withParameter_andMissingBinding_throwsSpecifyingMissingParam() {
    RuntimeException ex =
        assertThrows(
            MissingParameterBindingException.class,
            () -> ParameterizedQuery.from(":id", ImmutableMap.of()));
    assertThat(ex).hasMessageThat().isEqualTo("no binding provided for parameter :id");
  }

  @Test
  void withParameter_andMatchingBinding_returnsPreparableQuery() {
    ParameterizedQuery result =
        ParameterizedQuery.from("select * from foo where id = :id", ImmutableMap.of("id", 123));

    assertThat(result.getSql()).isEqualTo("select * from foo where id = ?");
  }

  @Test
  void withParameter_andMatchingBinding_returnsParameterList() {
    ParameterizedQuery result =
        ParameterizedQuery.from("select * from foo where id = :id", ImmutableMap.of("id", 123));

    assertThat(result.getParameters()).containsExactly(123);
  }

  @Test
  void withMultipleParameters_andMatchingBindings_returnsPreparableQueryAndParameterList() {
    ParameterizedQuery result =
        ParameterizedQuery.from(
            ":foo :bar :baz", ImmutableMap.of("foo", "FOO", "bar", "BAR", "baz", "BAZ"));

    assertThat(result.getSql()).isEqualTo("? ? ?");
    assertThat(result.getParameters()).containsExactly("FOO", "BAR", "BAZ").inOrder();
  }

  @Test
  void withDuplicateParameters_returnsDuplicateParametersInList() {
    ParameterizedQuery result =
        ParameterizedQuery.from(":foo :bar :foo", ImmutableMap.of("foo", "FOO", "bar", "BAR"));

    assertThat(result.getSql()).isEqualTo("? ? ?");
    assertThat(result.getParameters()).containsExactly("FOO", "BAR", "FOO").inOrder();
  }

  @Test
  void withParameterBoundToCollection_returnsBindSitePerValue() {
    ParameterizedQuery result =
        ParameterizedQuery.from(
            "select * from foo where id in (:foo)", ImmutableMap.of("foo", Arrays.asList(1, 2, 3)));

    assertThat(result.getSql()).isEqualTo("select * from foo where id in (?,?,?)");
    assertThat(result.getParameters()).containsExactly(1, 2, 3).inOrder();
  }

  @Test
  void withStringThatLooksLikeParameter_doesNotGetConfused() {
    ParameterizedQuery result =
        ParameterizedQuery.from(
            "':insideString' :outsideString", ImmutableMap.of("outsideString", 123));

    assertThat(result.getSql()).isEqualTo("':insideString' ?");
    assertThat(result.getParameters()).containsExactly(123);
  }

  @Test
  void withShorthandCastOperator_doesNotGetConfused() {
    ParameterizedQuery result =
        ParameterizedQuery.from(
            "not_a_param::string :a_param::jsonb", ImmutableMap.of("a_param", 123));

    assertThat(result.getSql()).isEqualTo("not_a_param::string ?::jsonb");
    assertThat(result.getParameters()).containsExactly(123);
  }
}
