package simplejdbc;

import static simplejdbc.Util.check;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import simplejdbc.SimpleJdbc.QueryResultExtractor;
import simplejdbc.SimpleJdbc.QueryRowResultExtractor;

public class QueryResultExtractors {

  public static <T> QueryResultExtractor<List<T>> list(QueryRowResultExtractor<T> rowExtractor) {
    return queryResult -> {
      List<T> returnValue = new ArrayList<>();
      while (queryResult.next()) {
        returnValue.add(rowExtractor.extract(queryResult));
      }
      return returnValue;
    };
  }

  public static <T> QueryResultExtractor<Optional<T>> first(
      QueryRowResultExtractor<T> rowExtractor) {
    return queryResult -> {
      if (queryResult.next()) {
        return Optional.of(rowExtractor.extract(queryResult));
      }
      return Optional.empty();
    };
  }

  public static <T> QueryResultExtractor<Optional<T>> maybeOne(
      QueryRowResultExtractor<T> rowExtractor) {
    return queryResult -> {
      if (!queryResult.next()) {
        return Optional.empty();
      }
      T result = rowExtractor.extract(queryResult);
      check(!queryResult.next(), "expected at most one result, but got multiple");
      return Optional.ofNullable(result);
    };
  }

  public static <T> QueryResultExtractor<T> exactlyOne(QueryRowResultExtractor<T> rowExtractor) {
    return queryResult -> {
      check(queryResult.next(), "expected exactly one result, but got none");
      T result = rowExtractor.extract(queryResult);
      check(!queryResult.next(), "expected exactly one result, but got multiple");
      return result;
    };
  }
}
