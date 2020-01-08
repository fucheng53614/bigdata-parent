package net.myvst.v2.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class AbstractConfig {

    protected Properties properties = new Properties();

    public AbstractConfig(String name) {
        InputStream inputStream = AbstractConfig.class.getResourceAsStream(name);
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
