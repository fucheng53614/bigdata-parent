package net.myvst.v2.task.impl;

import net.myvst.v2.db.DBPhoenixInstance;
import net.myvst.v2.task.Task;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public class MovieClassifyClick implements Task, Serializable {
    private DBPhoenixInstance db = DBPhoenixInstance.getInstance();
    private final String table = "bigdata_bi.vst_movie_classify_click";
    private final String create = "CREATE TABLE IF NOT EXISTS " + table + "( id VARCHAR PRIMARY KEY, date VARCHAR, cid VARCHAR, name VARCHAR, nameId VARCHAR, uv BIGINT, vv BIGINT)";

    @Override
    public void process(JavaRDD<String> rdd) throws Exception {

    }
}
