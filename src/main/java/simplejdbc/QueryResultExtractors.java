package simplejdbc;

import static simplejdbc.Util.check;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import simplejdbc.SimpleJdbc.QueryResultExtractor;

public class QueryResultExtractors {

  public static <T> QueryResultExtractor<List<T>> list(QueryResultExtractor<T> rowExtractor) {
    return queryResult -> {
      List<T> returnValue = new ArrayList<>();
      while (queryResult.next()) {
        returnValue.add(rowExtractor.extract(queryResult));
      }
      return returnValue;
    };
  }

  public static <T> QueryResultExtractor<Optional<T>> first(QueryResultExtractor<T> rowExtractor) {
    return queryResult -> {
      if (queryResult.next()) {
        return Optional.of(rowExtractor.extract(queryResult));
      }
      return Optional.empty();
    };
  }

  public static <T> QueryResultExtractor<Optional<T>> maybeOne(
      QueryResultExtractor<T> rowExtractor) {
    return first(
        queryResult -> {
          T returnValue = rowExtractor.extract(queryResult);
          check(!queryResult.next(), "expected at most one result, but got multiple");
          return returnValue;
        });
  }

  public static <T> QueryResultExtractor<T> exactlyOne(QueryResultExtractor<T> rowExtractor) {
    return queryResult ->
        first(
                qr -> {
                  T returnValue = rowExtractor.extract(qr);
                  check(!qr.next(), "expected exactly one result, but got multiple");
                  return returnValue;
                })
            .extract(queryResult)
            .orElseThrow(
                () -> new SimpleJdbcException("expected exactly one result, but got none"));
  }
}
