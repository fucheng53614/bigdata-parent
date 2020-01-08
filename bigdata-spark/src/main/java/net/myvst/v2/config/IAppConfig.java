package net.myvst.v2.config;

import java.util.Collection;

public interface IAppConfig {
    long getSecond();

    String getVideoDetailUrl();

    Collection<String> getTopics();
}
