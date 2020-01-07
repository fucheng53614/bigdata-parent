package net.myvst.v2.db;

import net.myvst.v2.utils.ConfigManager;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.KeyedHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * JDBC常用操作
 * 1.查询，2.执行（新增/更新）,3. 批量新增
 * <p>
 * <p>
 * //新增或者更新、新增或者累加、新增累加替换
 */
public class DBOperator {
    private DataSource dataSource = null;
    private int batch = 3000;

    public DBOperator() {
        try {
            Properties info = ConfigManager.getDbProperties();

            Object batch = info.get("batch");
            if (batch != null){
                this.batch = Integer.valueOf(String.valueOf(batch));
                info.remove("batch");
            }

            StringBuilder connectionProperties = new StringBuilder();
            for (Map.Entry<Object, Object> entry : info.entrySet()) {
                String key = (String)entry.getKey();
                if (key.startsWith("param.")){
                    String k = key.substring("param.".length());
                    connectionProperties.append(k).append("=").append(entry.getValue()).append(";");
                }
            }
            if (connectionProperties.length() > 0) connectionProperties.deleteCharAt(connectionProperties.length() - 1);

            System.out.println(connectionProperties);
            info.setProperty("connectionProperties", connectionProperties.toString());

            dataSource = BasicDataSourceFactory.createDataSource(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int update(String sql) throws SQLException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        return queryRunner.update(sql);
    }

    public Map<String, Map<String, Object>> query(String sql, String key) throws SQLException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        return queryRunner.query(sql, new KeyedHandler<>(key));
    }

    public int batch(String sql, Object[][] data, int batch) throws SQLException {
        int rows = 0;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            QueryRunner queryRunner = new QueryRunner();
            for (int i = 0; i < data.length; i++) {
                rows += queryRunner.update(connection, sql, data[i]);
                if ((i + 1) % batch == 0) {
                    connection.commit();
                }
            }
            connection.commit();
            connection.setAutoCommit(true);
        }
        return rows;
    }

    public int batch(String sql, Object[][] data) throws SQLException {
        return batch(sql, data, batch);
    }
}