package jdbc;

import lombok.Getter;
import lombok.Setter;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

@Getter
@Setter
public class CustomDataSource implements DataSource {
    private static final Logger logger = Logger.getLogger(CustomDataSource.class.getCanonicalName());

    private static CustomDataSource instance = null;
    private final String driver;
    private final String url;
    private final String name;
    private final String password;

    private final CustomConnector connector;

    private CustomDataSource(
            String driver,
            String url,
            String password,
            String name
    ) {
        this.driver = driver;
        this.url = url;
        this.name = name;
        this.password = password;
        this.connector = new CustomConnector();
    }

    public static CustomDataSource getInstance() {
        if (instance == null) {
            Properties properties = loadProperties();

            String driver = properties.getProperty("postgres.driver");
            String url = properties.getProperty("postgres.url");
            String name = properties.getProperty("postgres.name");
            String password = properties.getProperty("postgres.password");

            synchronized(CustomDataSource.class) {
                if (instance == null) {
                    try {
                        Class.forName(driver);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }

                    instance = new CustomDataSource(driver, url, password, name);
                }
            }
        }
        return instance;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("app.properties")) {
            if (is != null) {
                properties.load(is);
            } else {
                throw new IOException("Unable to load app.properties. File not found in resources");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize CustomDataSource", e);
        }
        return properties;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.getConnection(this.name, this.password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return this.connector.getConnection(this.url, username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
