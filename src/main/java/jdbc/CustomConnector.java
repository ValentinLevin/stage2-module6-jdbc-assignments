package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

public class CustomConnector {
    private static final Logger logger = Logger.getLogger(CustomConnector.class.getCanonicalName());
    public Connection getConnection(String url) throws SQLException {
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            logger.severe(e.getMessage());
            throw e;
        }
    }

    public Connection getConnection(String url, String user, String password) throws SQLException {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.severe(e.getMessage());
            throw e;
        }
    }
}
