package net.myvst.v2.utils;

import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.*;

public class ConfigManager {
    private static Properties kafkaProperties = null;
    private static Properties sparkProperties = null;
    private static Properties taskProperties = null;
    private static Properties dbProperties = null;
    private static Properties appProperties = null;

    public static enum Config{
        KAFKA,SPARK,TASK,DB,APP
    }

    static{
        try {
            kafkaProperties = new Properties();
            kafkaProperties.load(ConfigManager.class.getResourceAsStream("/kafka.properties"));

            sparkProperties = new Properties();
            sparkProperties.load(ConfigManager.class.getResourceAsStream("/spark.properties"));

            taskProperties = new Properties();
            taskProperties.load(ConfigManager.class.getResourceAsStream("/task.properties"));

            dbProperties = new Properties();
            dbProperties.load(ConfigManager.class.getResourceAsStream("/db.properties"));

            appProperties = new Properties();
            appProperties.load(ConfigManager.class.getResourceAsStream("/app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, Object> getKafkaProperties(){
        return (Map)kafkaProperties;
    }

    public static void setSparkConf(SparkConf conf){
        for (Map.Entry<Object, Object> entry : sparkProperties.entrySet()) {
            conf.set((String)entry.getKey(), (String)entry.getValue());
        }
    }

    public static Properties getDbProperties(){
        return dbProperties;
    }

    public static String getProperties(Config name, String key, String defaultValue){
        if (name == Config.KAFKA) return kafkaProperties.getProperty(key, defaultValue);
        else if (name == Config.APP) return appProperties.getProperty(key, defaultValue);
        else if (name == Config.TASK) return taskProperties.getProperty(key, defaultValue);
        else if (name == Config.SPARK) return sparkProperties.getProperty(key, defaultValue);
        else if (name == Config.DB) return dbProperties.getProperty(key, defaultValue);
        else return null;
    }

    public static String getProperties(Config name, String key){
        return getProperties(name, key, null);
    }

    public static List<String> getProperties2List(Config name, String key){
        String properties = getProperties(name, key, null);
        if (properties != null){
            return Arrays.asList(properties.split(","));
        }
        return null;
    }

    public static Long getProperties2Long(Config name, String key){
        String properties = getProperties(name, key, null);
        if (properties != null){
            return Long.valueOf(properties);
        }
        return null;
    }
}
