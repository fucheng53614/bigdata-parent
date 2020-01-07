package net.myvst.v2.manager;

import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.db.DBOperator;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class OffsetServiceImpl implements OffsetService {

    @Override
    public Map<TopicPartition, Long> loadOffset(DBOperator db, Collection<String> topics) throws SQLException {
        Map<TopicPartition, Long> topicAndPartitionMap = new HashMap<>();
        String where = "1=1";
        if (topics != null && !topics.isEmpty()) {
            where = " topic in ('" + StringUtils.join(topics, "','") + "')";
        }
        String query = "SELECT id, topic, partition, untilOffset FROM " + getTableName() + " WHERE ";
        Map<String, Map<String, Object>> data = db.query(query + where, "id");
        for (Map<String, Object> value : data.values()) {
            String topic = String.valueOf(value.get("topic"));
            int partition = Integer.valueOf(String.valueOf(value.get("partition")));
            long untilOffset = Long.valueOf(String.valueOf(value.get("untilOffset")));
            topicAndPartitionMap.put(new TopicPartition(topic, partition), untilOffset);
        }
        return topicAndPartitionMap;
    }

    @Override
    public String getTableName() {
        return "bigdata_config.vst_offset";
    }

    @Override
    public String createTableSql() {
        return "CREATE TABLE IF NOT EXISTS " + getTableName() + "(id VARCHAR PRIMARY KEY, topic VARCHAR, partition BIGINT, fromOffset bigint, untilOffset bigint)";
    }

    @Override
    public void store(DBOperator db, Object obj) throws SQLException {
        String insert = "UPSERT INTO " + getTableName() + "(id, topic, partition, fromOffset, untilOffset) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE fromOffset=?,untilOffset=?";

        OffsetRange[] offsetRanges = (OffsetRange[]) obj;

        int size = offsetRanges.length;
        Object[][] oo = new Object[size][];
        for (int i = 0; i < size; i++) {
            String topic = offsetRanges[i].topic();
            int partition = offsetRanges[i].partition();
            String id = MD5Hash.digest(topic + "," + partition).toString();
            long fromOffset = offsetRanges[i].fromOffset();
            long untilOffset = offsetRanges[i].untilOffset();
            log.info(String.format("topic partition fromOffset untilOffset [%s][%s][%s][%s]", topic, partition, fromOffset, untilOffset));

            Object[] objects = new Object[]{id, topic, partition, fromOffset, untilOffset, fromOffset, untilOffset};
            oo[i] = objects;
        }
        db.batch(insert, oo);
    }
}
