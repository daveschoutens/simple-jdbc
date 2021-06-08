package simplejdbc;

public class SimpleJdbcException extends RuntimeException {

  public SimpleJdbcException(Throwable cause) {
    super(cause);
  }

  public SimpleJdbcException(String message) {
    super(message);
  }

  public SimpleJdbcException(String message, Throwable cause) {
    super(message, cause);
  }
}
