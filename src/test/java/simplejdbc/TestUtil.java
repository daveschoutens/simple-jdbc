package simplejdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.function.Executable;

public class TestUtil {

  static void assertException(Executable fn, String message) {
    SimpleJdbcException ex = assertThrows(SimpleJdbcException.class, fn);
    assertThat(ex).hasMessageThat().isEqualTo(message);
  }
}
