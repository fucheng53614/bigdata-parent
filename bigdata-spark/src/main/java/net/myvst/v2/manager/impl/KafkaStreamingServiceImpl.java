package net.myvst.v2.manager.impl;

import com.alibaba.fastjson.JSONObject;
import net.myvst.v2.bean.StatCounter;
import net.myvst.v2.bean.UserRecord;
import net.myvst.v2.cache.IVideoAndTopicCache;
import net.myvst.v2.cache.impl.VideoAndTopicCacheImpl;
import net.myvst.v2.config.IAppConfig;
import net.myvst.v2.config.IKafkaConfig;
import net.myvst.v2.config.ISparkConfig;
import net.myvst.v2.db.ICommonDao;
import net.myvst.v2.manager.AbstractKafkaStreamingService;
import net.myvst.v2.task.TaskChain;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaStreamingServiceImpl extends AbstractKafkaStreamingService {

    private ICommonDao db;
    private IAppConfig appConfig;
    private ISparkConfig sparkConfig;
    private IKafkaConfig kafkaConfig;
    private String offsetTable = "bigdata_config.vst_offset";
    private TaskChain taskChain;

    public KafkaStreamingServiceImpl(IAppConfig appConfig, ISparkConfig sparkConfig, IKafkaConfig kafkaConfig, ICommonDao dao, TaskChain taskChain) {
        this.appConfig = appConfig;
        this.sparkConfig = sparkConfig;
        this.kafkaConfig = kafkaConfig;
        this.db = dao;
        this.taskChain = taskChain;
    }

    @Override
    public JavaStreamingContext createJavaStreamingContext() {
        SparkConf conf = new SparkConf();
        conf.registerKryoClasses(new Class[]{Object.class, StatCounter.class, JSONObject.class, UserRecord.class, Set.class, ConsumerRecord.class});
        sparkConfig.setSparkConf(conf);
        return new JavaStreamingContext(conf, Durations.seconds(appConfig.getSecond()));
    }

    @Override
    public JavaInputDStream createKafkaDStream(JavaStreamingContext javaStreamingContext) {
        Map<String, Object> kafkaParams = kafkaConfig.getKafkaParams();
        Collection<String> topics = appConfig.getTopics();
        Map<TopicPartition, Long> offsets = loadOffset(topics);
        return KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsets)
        );
    }

    @Override
    public IVideoAndTopicCache getVideoAndTopic() {
        return new VideoAndTopicCacheImpl(appConfig.getVideoDetailUrl());
    }

    @Override
    public void process(JavaRDD<String> rdd, OffsetRange[] offsetRanges) {
        try {
            this.taskChain.process(rdd);
            saveOffset(offsetRanges);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveOffset(OffsetRange[] offsetRanges) {
        String insert = "UPSERT INTO " + offsetTable + "(id, topic, partition, fromOffset, untilOffset) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE fromOffset=?,untilOffset=?";
        int size = offsetRanges.length;
        Object[][] oo = new Object[size][];
        for (int i = 0; i < size; i++) {
            String topic = offsetRanges[i].topic();
            int partition = offsetRanges[i].partition();
            String id = MD5Hash.digest(topic + "," + partition).toString();
            long fromOffset = offsetRanges[i].fromOffset();
            long untilOffset = offsetRanges[i].untilOffset();
            Object[] objects = new Object[]{id, topic, partition, fromOffset, untilOffset, fromOffset, untilOffset};
            oo[i] = objects;
        }
        try {
            db.insertBatch(insert, oo);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Map<TopicPartition, Long> loadOffset(Collection<String> topics){
        String create = "CREATE TABLE IF NOT EXISTS " + offsetTable + "(id VARCHAR PRIMARY KEY, topic VARCHAR, partition BIGINT, fromOffset bigint, untilOffset bigint)";
        try {
            db.create(create);
            Map<TopicPartition, Long> topicAndPartitionMap = new HashMap<>();
            String where = "1=1";
            if (topics != null && !topics.isEmpty()) {
                where = " topic in ('" + StringUtils.join(topics, "','") + "')";
            }
            String query = "SELECT id, topic, partition, untilOffset FROM " + offsetTable + " WHERE ";
            Map<String, Map<String, Object>> data = db.queryAllToMap(query + where, "id");
            for (Map<String, Object> value : data.values()) {
                String topic = String.valueOf(value.get("topic"));
                int partition = Integer.valueOf(String.valueOf(value.get("partition")));
                long untilOffset = Long.valueOf(String.valueOf(value.get("untilOffset")));
                topicAndPartitionMap.put(new TopicPartition(topic, partition), untilOffset);
            }
            return topicAndPartitionMap;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
