package jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SimpleJDBCRepository {
    private final Connection connection;

    private static final String createUserSQL = "insert into myusers(firstname, lastname, age) values(?, ?, ?)";
    private static final String updateUserSQL = "update myusers set firstname = ?, lastname = ?, age = ? where id = ?";
    private static final String deleteUserSQL = "delete from myusers where id = ?";
    private static final String findUserByIdSQL = "select id, lastname, firstname, age from myusers where id = ?";
    private static final String findUserByNameSQL = "select id, lastname, firstname, age from myusers where firstname = ?";
    private static final String findAllUserSQL = "select id, lastname, firstname, age from myusers";

    private PreparedStatement createUserStatement = null;
    private PreparedStatement updateUserStatement = null;
    private PreparedStatement deleteUserStatement = null;
    private PreparedStatement findUserByIdStatement = null;
    private PreparedStatement findUserByNameStatement = null;
    private PreparedStatement findAllUserStatement = null;

    public SimpleJDBCRepository() {
        try {
            this.connection = CustomDataSource.getInstance().getConnection();
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SimpleJDBCRepository(Connection connection) {
        try {
            this.connection = connection;
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Long createUser(User user) {
        try {
            if (this.createUserStatement == null) {
                this.createUserStatement = this.connection.prepareStatement(createUserSQL, PreparedStatement.RETURN_GENERATED_KEYS);
            }
            this.createUserStatement.setString(1, user.getFirstName());
            this.createUserStatement.setString(2, user.getLastName());
            this.createUserStatement.setInt(3, user.getAge());

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
            if (this.findUserByIdStatement == null) {
                this.findUserByIdStatement = this.connection.prepareStatement(findUserByIdSQL);
            }
            this.findUserByIdStatement.setLong(1, userId);
            return execStatement(this.findUserByIdStatement, this::userMapper);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User findUserByName(String userName) {
        try {
            if (this.findUserByNameStatement == null) {
                this.findUserByNameStatement = this.connection.prepareStatement(findUserByNameSQL);
            }
            this.findUserByNameStatement.setString(1, userName);
            return execStatement(this.findUserByNameStatement, this::userMapper);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> findAllUser() {
        try {
            if (this.findAllUserStatement == null) {
                this.findAllUserStatement = this.connection.prepareStatement(findAllUserSQL);
            }
            return execStatement(this.findAllUserStatement, this::userListMapper);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public User updateUser(User user) {
        try {
            if (this.updateUserStatement == null) {
                this.updateUserStatement = this.connection.prepareStatement(updateUserSQL);
            }
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
            if (this.deleteUserStatement == null) {
                this.deleteUserStatement = this.connection.prepareStatement(deleteUserSQL);
            }

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
