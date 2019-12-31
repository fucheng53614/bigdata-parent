package net.myvst.v2.task;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public interface Task extends Serializable {
    void process(JavaRDD<String> rdd) throws Exception;
}
