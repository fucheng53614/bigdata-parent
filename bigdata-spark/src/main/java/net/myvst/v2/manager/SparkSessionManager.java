package net.myvst.v2.manager;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionManager {
    private static transient SparkSession instance = null;

    private SparkSessionManager() {
    }

    public static SparkSession getInstance(SparkConf conf) {
        if (instance == null) {
            instance = SparkSession.builder().config(conf).getOrCreate();
        }
        return instance;
    }
}
