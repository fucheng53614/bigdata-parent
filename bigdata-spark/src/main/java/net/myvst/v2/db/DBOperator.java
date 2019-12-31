package net.myvst.v2.db;

import java.sql.*;
import java.util.Properties;

/**
 * JDBC常用操作
 * 1.查询，2.执行（新增/更新）,3. 批量新增
 */
public abstract class DBOperator {

    protected void initClass(String className) throws ClassNotFoundException {
        Class.forName(className);
    }

    public ResultSet executeQuery(Connection conn, String sql) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        return resultSet;
    }

    public int executorUpdate(Connection conn, String sql) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        return preparedStatement.executeUpdate();
    }

    public int executorBatchUpdate(Connection conn, String sql, Object[][] obj, int batch) throws SQLException {
        int sucNum = 0;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            for (int i = 0; i < obj.length; i++) {
                for (int j = 1; j <= obj[i].length; j++) {
                    preparedStatement.setObject(j, obj[i][j - 1]);
                }
                int ints = preparedStatement.executeUpdate();
                sucNum += ints;
                if (i + 1 % batch == 0) {
                    conn.commit();
                }
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
        return sucNum;
    }

    protected Connection getConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    protected Connection getConnection(String url, Properties info) throws SQLException {
        return DriverManager.getConnection(url, info);
    }

    public abstract Connection getConnection() throws SQLException;
}