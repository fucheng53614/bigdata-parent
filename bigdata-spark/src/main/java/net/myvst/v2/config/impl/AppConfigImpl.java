package net.myvst.v2.config.impl;

import net.myvst.v2.config.AbstractConfig;
import net.myvst.v2.config.IAppConfig;

import java.util.Arrays;
import java.util.Collection;

public class AppConfigImpl extends AbstractConfig implements IAppConfig {

    public AppConfigImpl() {
        super("/app.properties");
    }

    @Override
    public long getSecond() {
        return Long.valueOf(properties.getProperty("app.seconds", "10"));
    }

    @Override
    public String getVideoDetailUrl() {
        return properties.getProperty("app.video.details.url");
    }

    @Override
    public Collection<String> getTopics() {
        String property = properties.getProperty("app.topics");
        if (property == null) {
            throw new IllegalArgumentException("app.topics not exists.");
        }
        return Arrays.asList(property.split(","));
    }
}
