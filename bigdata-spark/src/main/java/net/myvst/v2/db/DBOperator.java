package net.myvst.v2.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC常用操作
 * 1.查询，2.执行（新增/更新）,3. 批量新增
 */
public abstract class DBOperator {

    public static DBOperator getDBInstance(String db) {
        if ("mysql".equals(db)) {
            return DBMysqlInstance.getInstance();
        } else if ("phoenix".equals(db)) {
            return DBPhoenixInstance.getInstance();
        } else {
            throw new IllegalArgumentException(db + " not found.");
        }
    }

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

    /**
     * 新增或者更新
     *
     * @throws SQLException
     */
    public int insertOrUpdate(Connection conn, String table, String id, Map<String, Map<String, Object>> data, List<String> insert, List<String> update, int batch) throws SQLException {
        return insertOrUpdateOrAccumulate(conn, table, id, data, insert, update, batch, false);
    }

    /**
     * 新增或者累加
     *
     * @throws SQLException
     */
    public int insertOrAccumulate(Connection conn, String table, String id, Map<String, Map<String, Object>> data, List<String> insert, List<String> update, int batch) throws SQLException {
        return insertOrUpdateOrAccumulate(conn, table, id, data, insert, update, batch, true);
    }


    private void wrapBatch(List<String> insert, StringBuilder join, StringBuilder param) {
        if (insert.size() > 1) {
            join.append(insert.get(0));
            param.append("?");
            for (int i = 1; i < insert.size(); i++) {
                join.append(",").append(insert.get(i));
                param.append(",").append("?");
            }
        }
    }

    /**
     * 新增或者修改或者累加
     * 1。isAcc: true 新增或者累加
     * 2. isAcc: false 新增或者修改
     *
     * @param conn
     * @param table
     * @param id
     * @param data
     * @param insert
     * @param update
     * @param batch
     * @param isAcc
     * @return
     * @throws SQLException
     */
    private int insertOrUpdateOrAccumulate(Connection conn, String table, String id, Map<String, Map<String, Object>> data, List<String> insert, List<String> update, int batch, boolean isAcc) throws SQLException {
        List<Object[]> updateObject = new ArrayList<>();
        List<Object[]> insertObject = new ArrayList<>();

        StringBuilder insertJoin = new StringBuilder();
        StringBuilder insertParam = new StringBuilder();
        wrapBatch(insert, insertJoin, insertParam);

        StringBuilder updateJoin = new StringBuilder();
        StringBuilder updateParam = new StringBuilder();
        wrapBatch(update, updateJoin, updateParam);

        for (Map.Entry<String, Map<String, Object>> entry : data.entrySet()) {
            String idValue = entry.getKey();
            Map<String, Object> mapValue = entry.getValue();
            String query = "SELECT " + updateJoin + " FROM " + table + " WHERE " + id + " = '" + idValue + "'";
            ResultSet resultSet = executeQuery(conn, query);
            if (resultSet.next()) {
                Object[] uo = new Object[update.size()];
                if (isAcc) {
                    uo[0] = resultSet.getString(1);
                    for (int i = 1; i < update.size(); i++) {
                        Object curObject = mapValue.get(update.get(i));
                        if (curObject instanceof String) {
                            String oldValue = resultSet.getString(i + 1);
                            uo[i] = oldValue + "," + curObject;
                        } else if (curObject instanceof Long) {
                            long oldValue = resultSet.getLong(i + 1);
                            long curValue = (long) mapValue.get(update.get(i));
                            uo[i] = curValue + oldValue;
                        }
                    }
                } else {
                    for (int i = 0; i < update.size(); i++) {
                        uo[i] = mapValue.get(update.get(i));
                    }
                }
                updateObject.add(uo);
            } else {
                Object[] io = new Object[insert.size()];
                for (int i = 0; i < insert.size(); i++) {
                    io[i] = mapValue.get(insert.get(i));
                }
                insertObject.add(io);
            }
        }
        int result = executorBatch(conn, table, insertObject, insertJoin, insertParam, batch);
        result += executorBatch(conn, table, updateObject, updateJoin, updateParam, batch);
        return result;
    }

    protected int executorBatch(Connection conn, String table, List<Object[]> objects, StringBuilder join, StringBuilder param, int batch) throws SQLException {
        String insertSql = "INSERT INTO " + table + "( " + join + " ) VALUES ( " + param + " )";
        return executorBatchUpdate(conn, insertSql, objects.toArray(new Object[0][]), batch);
    }

    protected Connection getConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    protected Connection getConnection(String url, Properties info) throws SQLException {
        return DriverManager.getConnection(url, info);
    }

    public abstract Connection getConnection() throws SQLException;
}