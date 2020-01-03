package net.myvst.v2.task;

import net.myvst.v2.db.DataMapping;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public interface Task extends DataMapping, Serializable {
    Object process(JavaRDD<String> rdd) throws Exception;
}
