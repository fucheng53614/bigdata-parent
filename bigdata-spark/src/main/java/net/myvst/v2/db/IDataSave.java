package net.myvst.v2.db;

import java.sql.SQLException;

public interface IDataSave {
    String getTableName();

    String createTableSql();

    void store(ICommonDao db, Object obj) throws SQLException;
}
