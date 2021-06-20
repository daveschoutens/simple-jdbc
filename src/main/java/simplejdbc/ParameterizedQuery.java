package simplejdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParameterizedQuery {

  private static final Pattern PARAM_REGEX = Pattern.compile("(?!\\B'[^']*):(\\w+)(?![^']*'\\B)");

  @SuppressWarnings("rawtypes")
  static ParameterizedQuery from(String query, Map<String, ?> bindings) {
    Objects.requireNonNull(query, "query is required, but was null");
    Objects.requireNonNull(bindings, "bindings required, but was null");

    StringBuffer sb = new StringBuffer();
    List<Object> parameters = new ArrayList<>();
    Matcher m = PARAM_REGEX.matcher(query);
    while (m.find()) {
      String param = m.group(1);
      if (!bindings.containsKey(param)) {
        throw new MissingParameterBindingException(param);
      }
      Object value = bindings.get(param);
      if (value instanceof Collection) {
        StringJoiner multiParam = new StringJoiner(",");
        for (Object subValue : (Collection) value) {
          multiParam.add("?");
          parameters.add(subValue);
        }
        m.appendReplacement(sb, multiParam.toString());
      } else {
        parameters.add(value);
        m.appendReplacement(sb, "?");
      }
    }
    m.appendTail(sb);
    return new ParameterizedQuery(sb.toString(), parameters);
  }

  private final String query;
  private final List<Object> parameters;

  ParameterizedQuery(String query, List<Object> parameters) {
    this.query = query;
    this.parameters = parameters;
  }

  public String getSql() {
    return query;
  }

  public List<Object> getParameters() {
    return parameters;
  }

  public static class MissingParameterBindingException extends SimpleJdbcException {
    MissingParameterBindingException(String param) {
      super(String.format("no binding provided for parameter :%s", param));
    }
  }
}
