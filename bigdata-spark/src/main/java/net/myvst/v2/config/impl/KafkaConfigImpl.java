package net.myvst.v2.config.impl;

import net.myvst.v2.config.AbstractConfig;
import net.myvst.v2.config.IKafkaConfig;

import java.util.Map;

public class KafkaConfigImpl extends AbstractConfig implements IKafkaConfig {

    public KafkaConfigImpl() {
        super("/kafka.properties");
    }

    @Override
    public Map<String, Object> getKafkaParams() {
        return (Map)properties;
    }
}
