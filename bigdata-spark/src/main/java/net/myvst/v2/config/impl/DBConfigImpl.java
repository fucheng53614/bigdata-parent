package net.myvst.v2.config.impl;

import net.myvst.v2.config.AbstractConfig;
import net.myvst.v2.config.IDBConfig;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class DBConfigImpl extends AbstractConfig implements IDBConfig {

    private int batch = 3000;

    public DBConfigImpl() {
        super("/db.properties");

        String prefix = "param.";
        StringBuilder connectionProperties = new StringBuilder();
        Iterator<Map.Entry<Object, Object>> iterator = properties.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Object, Object> entry = iterator.next();
            String key = (String)entry.getKey();
            if (key.startsWith(prefix)){
                connectionProperties.append(key.substring(prefix.length())).append("=").append(entry.getValue()).append(";");
                iterator.remove();
            }
        }

        properties.setProperty("connectionProperties", connectionProperties.toString());

        String batch = properties.getProperty("batch");
        if (batch != null){
            this.batch = Integer.valueOf(batch);
            properties.remove("batch");
        }
    }

    @Override
    public Properties getDBProperties() {
        return properties;
    }

    @Override
    public int getBatch() {
        return batch;
    }
}
