package net.myvst.v2.db.impl;

import net.myvst.v2.config.IDBConfig;
import net.myvst.v2.db.ICommonDao;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.KeyedHandler;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class PhoenixDaoImpl implements ICommonDao {

    private QueryRunner queryRunner;
    private DataSource dataSource;
    private IDBConfig config;

    public PhoenixDaoImpl(IDBConfig config) {
        try {
            this.config = config;
            this.dataSource = BasicDataSourceFactory.createDataSource(config.getDBProperties());
            this.queryRunner = new QueryRunner(dataSource);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Map<String, Object>> queryAllToMap(String sql, String idCol) throws SQLException {
        return this.queryRunner.query(sql, new KeyedHandler<>(idCol));
    }

    @Override
    public int insert(String sql, Object[] objects) throws SQLException {
        return queryRunner.update(sql, objects);
    }

    @Override
    public int create(String sql) throws SQLException {
        if (StringUtils.isEmpty(sql)) return -1;
        return queryRunner.update(sql);
    }

    @Override
    public int insertBatch(String sql, Object[][] objects, int batch) throws SQLException {
        int rows = 0;
        try (Connection connection = this.dataSource.getConnection()) {
            connection.setAutoCommit(false);
            QueryRunner queryRunner = new QueryRunner();
            for (int i = 0; i < objects.length; i++) {
                rows += queryRunner.update(connection, sql, objects[i]);
                if ((i + 1) % batch == 0) {
                    connection.commit();
                }
            }
            connection.commit();
            connection.setAutoCommit(true);
        }
        return rows;
    }

    @Override
    public int insertBatch(String sql, Object[][] objects) throws SQLException {
        return insertBatch(sql, objects, config.getBatch());
    }
}
