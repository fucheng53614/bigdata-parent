package net.myvst.v2.db;

import java.sql.SQLException;

public interface DataMapping {
    String getTableName();

    String createTableSql();

    void store(DBOperator db, Object obj) throws SQLException;
}
