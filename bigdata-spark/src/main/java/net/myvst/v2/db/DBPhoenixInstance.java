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

    @Override
    protected int executorBatch(Connection conn, String table, List<Object[]> objectList, StringBuilder join, StringBuilder param, int batch) throws SQLException {
        String insertSql = "UPSERT INTO " + table + "( " + join + " ) VALUES ( " + param + " )";
        return executorBatchUpdate(conn, insertSql, objectList.toArray(new Object[0][]), batch);
    }
}
