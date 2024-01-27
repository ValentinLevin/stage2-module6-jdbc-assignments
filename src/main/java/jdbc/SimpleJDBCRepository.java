package jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class SimpleJDBCRepository {

    private Connection connection = null;

    private static final String createUserSQL =
            "insert into myusers(firstname, lastname, age) values(?, ?, ?)";

    private static final String updateUserSQL =
            "update myusers " +
                    "set " +
                    "firstname = ?, " +
                    "lastname = ?, " +
                    "age = ? " +
                    "where id = ?";

    private static final String deleteUserSQL = "delete from myusers where id = ?";
    private static final String findUserByIdSQL = "select id, lastname, firstname, age from myusers where id = ?";
    private static final String findUserByNameSQL = "select id, lastname, firstname, age from myusers where lastname = ?";
    private static final String findAllUserSQL = "select id, lastname, firstname, age from myusers";

    private PreparedStatement createUserStatement;
    private PreparedStatement updateUserStatement;
    private PreparedStatement deleteUserStatement;
    private PreparedStatement findUserByIdStatement;
    private PreparedStatement findUserByNameStatement;
    private PreparedStatement findAllUserStatement;

    public SimpleJDBCRepository() {
        try {
            this.connection = CustomDataSource.getInstance().getConnection();
            this.connection.setAutoCommit(false);
            prepareStatements();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SimpleJDBCRepository(Connection connection) {
        try {
            this.connection = connection;
            this.connection.setAutoCommit(false);
            prepareStatements();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void prepareStatements() throws SQLException {
        this.createUserStatement = this.connection.prepareStatement(createUserSQL, PreparedStatement.RETURN_GENERATED_KEYS);
        this.updateUserStatement = this.connection.prepareStatement(updateUserSQL);
        this.deleteUserStatement = this.connection.prepareStatement(deleteUserSQL);
        this.findUserByIdStatement = this.connection.prepareStatement(findUserByIdSQL);
        this.findUserByNameStatement = this.connection.prepareStatement(findUserByNameSQL);
        this.findAllUserStatement = this.connection.prepareStatement(findAllUserSQL);
    }

    public Long createUser(User user) {
        try {
            this.createUserStatement.setString(1, user.getFirstName());
            this.createUserStatement.setString(2, user.getLastName());
            this.createUserStatement.setInt(3, user.getAge());
            this.connection.setAutoCommit(false);
            try {
                this.createUserStatement.executeUpdate();
                this.connection.commit();

                ResultSet generatedKeys = this.createUserStatement.getGeneratedKeys();
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                } else {
                    return 0L;
                }
            } catch (Exception e) {
                this.connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User findUserById(Long userId) {
        try {
            this.findUserByIdStatement.setLong(1, userId);
            return execStatement(this.findUserByIdStatement, this::userMapper);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User findUserByName(String userName) {
        try {
            this.findUserByNameStatement.setString(1, userName);
            return execStatement(this.findUserByNameStatement, this::userMapper);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> findAllUser() {
        return execStatement(this.findAllUserStatement, this::userListMapper);
    }

    public User updateUser(User user) {
        try {
            this.updateUserStatement.setString(1, user.getFirstName());
            this.updateUserStatement.setString(2, user.getLastName());
            this.updateUserStatement.setInt(3, user.getAge());
            this.updateUserStatement.setLong(4, user.getId());
            try {
                this.updateUserStatement.execute();
                this.connection.commit();
                return user;
            } catch (Exception e) {
                this.connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteUser(Long userId) {
        try {
            try {
                this.deleteUserStatement.setLong(1, userId);
                this.deleteUserStatement.execute();
                this.connection.commit();
            } catch (Exception e) {
                this.connection.rollback();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private <R> R execStatement(PreparedStatement st, Function<ResultSet, R> func) {
        try (ResultSet rs = st.executeQuery()) {
            return func.apply(rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private User userMapper(ResultSet rs) {
        User user = new User();
        try {
            if (rs.next()) {
                user.setId(rs.getLong("id"));
                user.setFirstName(rs.getString("firstname"));
                user.setLastName(rs.getString("lastname"));
                user.setAge(rs.getInt("age"));
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("An error occurred while retrieving query result values", e);
        }
        return user;
    }

    private List<User> userListMapper(ResultSet rs) {
        List<User> users = new ArrayList<>();

        User user;
        do {
            user = userMapper(rs);
            if (user != null) {
                users.add(user);
            }
        } while (user != null);

        return users;
    }
}
