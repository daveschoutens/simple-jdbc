package simplejdbc;

import java.util.regex.Pattern;

public class Util {

  static final Pattern TABLE_NAME_REGEX = Pattern.compile("^[\\w]+(\\.[\\w]+)?$");
  static final Pattern COLUMN_NAME_REGEX = Pattern.compile("^\\w+$");
  static final Pattern PARAMETER_NAME_REGEX = COLUMN_NAME_REGEX;

  static void check(boolean predicate, String message) {
    if (!predicate) {
      throw new SimpleJdbcException(message);
    }
  }
}
