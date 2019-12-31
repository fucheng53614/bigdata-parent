package net.myvst.v2.db;

import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.manager.ConfigManager;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * MySQL操作
 */
@Slf4j
public class DBMysqlInstance extends DBOperator {
    private static DBMysqlInstance instance = new DBMysqlInstance();
    private DBMysqlInstance() {
        try {
            super.initClass(ConfigManager.getInstance().getString(ConfigManager.MYSQL_JDBC_DRIVER));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static DBMysqlInstance getInstance(){
        return instance;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return super.getConnection(
                ConfigManager.getInstance().getString(ConfigManager.MYSQL_JDBC_URL),
                ConfigManager.getInstance().getString(ConfigManager.MYSQL_JDBC_USER),
                ConfigManager.getInstance().getString(ConfigManager.MYSQL_JDBC_PASSWORD));
    }
}
