package net.myvst.v2.task;

import net.myvst.v2.db.DBOperator;
import net.myvst.v2.manager.ConfigManager;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public interface Task extends Serializable {
    DBOperator db = DBOperator.getDBInstance(ConfigManager.getInstance().getString(ConfigManager.SAVE_JDBC));
    void process(JavaRDD<String> rdd) throws Exception;
}
