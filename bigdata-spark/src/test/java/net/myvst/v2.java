package net.myvst;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class v2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.set("spark.hadoop.fs.default.name", "hdfs://fuch0:9000");
        conf.set("spark.master","local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        spark.read().parquet("/cibnvst/data/parquet/20191215/*/*/click.parquet").write().json("/tmp/click/20191215_JSON");
        spark.read().parquet("/cibnvst/data/parquet/20191215/*/*/click.parquet").write().parquet("/tmp/click/20191215_PARQUET");
        spark.close();
    }
}
