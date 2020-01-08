package net.myvst.v2.task;

import net.myvst.v2.db.IDataSave;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public interface Task extends IDataSave, Serializable {
    Object process(JavaRDD<String> rdd) throws Exception;
}
