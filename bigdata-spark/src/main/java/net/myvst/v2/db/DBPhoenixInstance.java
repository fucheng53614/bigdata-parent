package net.myvst.v2.db;

import net.myvst.v2.manager.ConfigManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Phoenix操作
 */
public class DBPhoenixInstance extends DBOperator {
    private static DBPhoenixInstance instance = new DBPhoenixInstance();

    private DBPhoenixInstance() {
        try {
            super.initClass(ConfigManager.getInstance().getString(ConfigManager.PHOENIX_JDBC_DRIVER));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static DBPhoenixInstance getInstance(){
        return instance;
    }


    @Override
    public Connection getConnection() throws SQLException {
        Properties info = new Properties();
        info.put("phoenix.mutate.batchSize", "15000000");
        info.put("phoenix.mutate.maxSize", "200000");
        info.put("phoenix.mutate.maxSizeBytes", "1048576000");
        return super.getConnection(ConfigManager.getInstance().getString(ConfigManager.PHOENIX_JDBC_URL), info);
    }

    /**
     * 新增或者更新
     * @throws SQLException
     */
    public int insertOrUpdate(Connection conn, String table, String id, Map<String, Map<String, Object>> data, List<String> insert, List<String> update, int batch) throws SQLException {
        return insertOrUpdateOrAccumulate(conn, table, id, data, insert, update, batch, false);
    }

    /**
     * 新增或者累加
     * @throws SQLException
     */
    public int insertOrAccumulate(Connection conn, String table, String id, Map<String, Map<String, Object>> data, List<String> insert, List<String> update, int batch) throws SQLException {
        return insertOrUpdateOrAccumulate(conn, table, id, data, insert, update, batch, true);
    }

    private int executorBatch(Connection conn, String table, List<Object[]> objectList, StringBuilder join, StringBuilder param, int batch) throws SQLException {
        String insertSql = "UPSERT INTO " + table + "( " + join + " ) VALUES ( " + param + " )";
        return executorBatchUpdate(conn, insertSql, objectList.toArray(new Object[0][]), batch);
    }

    private void wrapBatch(List<String> insert, StringBuilder join, StringBuilder param){
        if (insert.size() > 1) {
            join.append(insert.get(0));
            param.append("?");
            for (int i = 1; i < insert.size(); i++) {
                join.append(",").append(insert.get(i));
                param.append(",").append("?");
            }
        }
    }

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
                }else{
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
}
