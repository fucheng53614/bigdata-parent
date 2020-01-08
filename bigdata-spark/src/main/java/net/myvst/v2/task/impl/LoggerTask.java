package net.myvst.v2.task.impl;

import net.myvst.v2.db.ICommonDao;
import net.myvst.v2.task.Task;
import org.apache.spark.api.java.JavaRDD;

import java.sql.SQLException;

public class LoggerTask implements Task {
    @Override
    public Object process(JavaRDD<String> rdd) throws Exception {
        rdd.foreachPartition(it -> {
            while (it.hasNext()) {
                System.out.println(it.next());
            }
        });
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public String createTableSql() {
        return null;
    }

    @Override
    public void store(ICommonDao db, Object obj) throws SQLException {

    }
}
