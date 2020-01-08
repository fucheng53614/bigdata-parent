package net.myvst.v2.config;

import java.util.Properties;

public interface IDBConfig {
    Properties getDBProperties();

    int getBatch();
}
