package net.myvst.v2.manager;

import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConfigManager {
    private static ConfigManager instance = new ConfigManager();

    public static final String SPARK_STREAMING_SECONDS = "spark.streaming.seconds";
    public static final String SPARK_STREAMING_NAME = "spark.streaming.name";

    public static final String KAFKA_TOPICS = "kafka.topics";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String KAFKA_MAX_POLL_RECORDS = "kafka.max.poll.records";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";

    public static final String SAVE_JDBC = "save.jdbc";

    public static final String PHOENIX_JDBC_DRIVER = "phoenix.jdbc.driver";
    public static final String PHOENIX_JDBC_URL = "phoenix.jdbc.url";

    public static final String MYSQL_JDBC_DRIVER = "mysql.jdbc.driver";
    public static final String MYSQL_JDBC_URL = "mysql.jdbc.url";
    public static final String MYSQL_JDBC_USER = "mysql.jdbc.user";
    public static final String MYSQL_JDBC_PASSWORD = "mysql.jdbc.password";

    public static final String VIDEO_DETAILS_URL = "video.details.url";

    private static final String SYSTEM_CONF = "bigdata.conf";

    public static ConfigManager getInstance() {
        return instance;
    }

    private Properties conf = new Properties();

    private ConfigManager() {
        String file = "config.properties";
        String config = System.getProperty(SYSTEM_CONF);
        try {
            if (!StringUtils.isEmpty(config)) {
                File f = ConfigurationUtils.fileFromURL(new URL(config));
                List<String> lines = FileUtils.readLines(f);
                for (String line : lines) {
                    String trimLine = line.trim();
                    if (!StringUtils.isEmpty(trimLine) && !trimLine.startsWith("#")) {
                        String[] split = line.split("=");
                        conf.put(split[0].trim(), split[1].trim());
                    }
                }
            } else {
                InputStream inputStream = ConfigManager.class.getResourceAsStream("/" + file);
                conf.load(inputStream);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getString(String key, String defaultValue){ return conf.getProperty(key, defaultValue); }

    public String getString(String key){ return conf.getProperty(key); }

    public Long getLong(String key){ return Long.valueOf(getString(key)); }

    public int getInt(String key){ return Integer.valueOf(getString(key)); }

    public List<String> getList(String key){
        String v = getString(key);
        if (!StringUtils.isEmpty(v)){
            return Arrays.asList(v.split(","));
        }else{
            return Collections.emptyList();
        }
    }
}
