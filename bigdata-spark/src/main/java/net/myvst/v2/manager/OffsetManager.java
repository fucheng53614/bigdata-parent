package net.myvst.v2.manager;

import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.db.DBPhoenixInstance;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class OffsetManager {
    private static OffsetManager instance = new OffsetManager();
    private DBPhoenixInstance db = DBPhoenixInstance.getInstance();
    private final String table = "bigdata_config.vst_offset";

    private OffsetManager() {
        try {
            createTable();
        } catch (SQLException e) {
             e.printStackTrace();
        }
    }

    public static OffsetManager getInstance() {
        return instance;
    }

    private void createTable() throws SQLException {
        Connection connection = null;
        try {
            connection = db.getConnection();
            String create = "CREATE TABLE IF NOT EXISTS " + table + "(id VARCHAR PRIMARY KEY, topic VARCHAR, partition BIGINT, fromOffset bigint, untilOffset bigint)";
            db.executorUpdate(connection, create);
        } finally {
            if (connection != null) connection.close();
        }
    }

    public void saveOffset(OffsetRange[] offsetRanges) throws SQLException {
        Connection connection = null;
        try {
            connection = db.getConnection();
            Map<String, Map<String, Object>> data = new HashMap<>();
            for (int i = 0; i < offsetRanges.length; i++) {
                String topic = offsetRanges[i].topic();
                int partition = offsetRanges[i].partition();
                String id = topic + "," + partition;
                long fromOffset = offsetRanges[i].fromOffset();
                long untilOffset = offsetRanges[i].untilOffset();
                log.info(String.format("topic partition fromOffset untilOffset [%s][%s][%s][%s]", topic, partition, fromOffset, untilOffset));

                Map<String, Object> mapValue = data.computeIfAbsent(id, k -> new HashMap<>());
                mapValue.put("id", id);
                mapValue.put("topic", topic);
                mapValue.put("partition", partition);
                mapValue.put("fromOffset", fromOffset);
                mapValue.put("untilOffset", untilOffset);
            }
            int batch = 1000;
            db.insertOrUpdate(connection, table, "id", data, Arrays.asList("id", "topic", "partition", "fromOffset", "untilOffset"), Arrays.asList("id", "fromOffset", "untilOffset"), batch);
        } finally {
            if (connection != null) connection.close();
        }
    }

    public Map<TopicPartition, Long> loadOffset(Collection<String> topics) throws SQLException {
        Connection connection = null;
        Map<TopicPartition, Long> topicAndPartitionMap = new HashMap<>();
        try {
            String where = "1=1";
            if (topics != null && !topics.isEmpty()) {
                where = " topic in ('" + StringUtils.join(topics, "','") + "')";
            }
            connection = db.getConnection();
            String query = "SELECT topic, partition, fromOffset, untilOffset FROM " + table + " WHERE ";
            ResultSet resultSet = db.executeQuery(connection, query + where);
            while (resultSet.next()) {
                String topic = resultSet.getString(1);
                int partition = resultSet.getInt(2);
                long fromOffset = resultSet.getLong(3);
                long untilOffset = resultSet.getLong(4);
                topicAndPartitionMap.put(new TopicPartition(topic, partition), untilOffset);
            }
            resultSet.close();
        } finally {
            if (connection != null) connection.close();
        }
        return topicAndPartitionMap;
    }

}
