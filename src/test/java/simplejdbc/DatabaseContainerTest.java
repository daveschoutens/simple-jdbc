package simplejdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class DatabaseContainerTest {

  private static PostgreSQLContainer<?> postgreSQLContainer;
  private static DataSource dataSource;

  @BeforeAll
  static void setupDataSource() {
    postgreSQLContainer = new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"));
    postgreSQLContainer.start();
    dataSource = getDataSource(postgreSQLContainer);
  }

  private static DataSource getDataSource(JdbcDatabaseContainer<?> container) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(container.getJdbcUrl());
    hikariConfig.setUsername(container.getUsername());
    hikariConfig.setPassword(container.getPassword());
    hikariConfig.setDriverClassName(container.getDriverClassName());
    return new HikariDataSource(hikariConfig);
  }

  protected DataSource getDataSource() {
    return dataSource;
  }
}
