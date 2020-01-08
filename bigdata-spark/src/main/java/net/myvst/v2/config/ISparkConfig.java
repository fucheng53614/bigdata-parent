package net.myvst.v2.config;

import org.apache.spark.SparkConf;

public interface ISparkConfig {
    void setSparkConf(SparkConf conf);
}
