package net.myvst.v2.db;

import java.sql.SQLException;
import java.util.Map;

public interface ICommonDao {
    Map<String, Map<String, Object>> queryAllToMap(String sql, String idCol) throws SQLException;

    int insert(String sql, Object[] objects) throws SQLException;

    int create(String sql) throws SQLException;

    int insertBatch(String sql, Object[][] objects, int batch) throws SQLException;

    int insertBatch(String sql, Object[][] objects) throws SQLException;
}
