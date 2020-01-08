package net.myvst.v2.config.impl;

import net.myvst.v2.config.AbstractConfig;
import net.myvst.v2.config.ISparkConfig;
import org.apache.spark.SparkConf;

import java.util.Map;

public class SparkConfigImpl extends AbstractConfig implements ISparkConfig {

    public SparkConfigImpl() {
        super("/spark.properties");
    }

    @Override
    public void setSparkConf(SparkConf conf) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            conf.set((String)entry.getKey(), (String)entry.getValue());
        }
    }
}
