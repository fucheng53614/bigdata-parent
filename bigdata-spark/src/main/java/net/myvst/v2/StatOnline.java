package net.myvst.v2;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.bean.StatCounter;
import net.myvst.v2.bean.UserRecord;
import net.myvst.v2.cache.VideoAndTopicCache;
import net.myvst.v2.manager.ConfigManager;
import net.myvst.v2.manager.OffsetManager;
import net.myvst.v2.task.TaskChain;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.sql.*;
import java.util.*;

/**
 * 在线处理任务
 */
@Slf4j
public class StatOnline {

    static {
        Logger.getLogger("org").setLevel(Level.WARN);
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        Collection<String> topics = ConfigManager.getInstance().getList(ConfigManager.KAFKA_TOPICS);

        SparkConf conf = new SparkConf();
        conf.setAppName(ConfigManager.getInstance().getString(ConfigManager.SPARK_STREAMING_NAME));
        conf.registerKryoClasses(new Class[]{Object.class, StatCounter.class, JSONObject.class, UserRecord.class, Set.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.streaming.kafka.maxPollCount", "30000");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "60000");
        conf.set("spark.streaming.backpressure.enabled", "true");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", ConfigManager.getInstance().getString(ConfigManager.KAFKA_BOOTSTRAP_SERVERS));
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id",ConfigManager.getInstance().getString(ConfigManager.KAFKA_GROUP_ID));
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("max.poll.records", ConfigManager.getInstance().getString(ConfigManager.KAFKA_MAX_POLL_RECORDS));


        Map<TopicPartition, Long> topicPartitionMap = OffsetManager.getInstance().loadOffset(topics);

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(ConfigManager.getInstance().getLong(ConfigManager.SPARK_STREAMING_SECONDS)));
        Broadcast<VideoAndTopicCache> broadcast = ssc.sparkContext().broadcast(VideoAndTopicCache.getInstance());

        JavaInputDStream<ConsumerRecord<String, String>> directStream;

        if (topicPartitionMap.isEmpty()) {
            log.info("load latest offset");
            directStream = KafkaUtils.createDirectStream(ssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams)
            );
        } else {
            log.info("load db offset");
            directStream = KafkaUtils.createDirectStream(ssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Assign(topicPartitionMap.keySet(), kafkaParams, topicPartitionMap)
            );
        }

        directStream.foreachRDD(rdd -> {
            OffsetRange[] untilOffsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            JavaRDD<String> preProcessRDD = preProcess(rdd.map(ConsumerRecord::value), broadcast);
            preProcessRDD.cache();

            TaskChain.getInstance().process(preProcessRDD);

            preProcessRDD.unpersist();
            OffsetManager.getInstance().saveOffset(untilOffsetRanges);
        });
        ssc.start();
        ssc.awaitTermination();

    }

    public static JavaRDD<String> preProcess(JavaRDD<String> rdd, Broadcast<VideoAndTopicCache> broadcast) {
        return rdd.map(s -> {
            JSONObject jsonObject = JSONObject.parseObject(s);

            String nameId = jsonObject.getString("nameId");
            if (!StringUtils.isEmpty(nameId)) {
                JSONObject video = broadcast.value().getVideo(nameId);
                if (video != null) {
                    jsonObject.put("cid", video.get("cid"));
                    jsonObject.put("specId", video.get("specialType"));
                    jsonObject.put("name", video.get("title"));
                    jsonObject.put("actor", video.get("actor"));
                    jsonObject.put("area", video.get("area"));
                    jsonObject.put("cat", video.get("cat"));
                    jsonObject.put("director", video.get("director"));
                    jsonObject.put("year", video.get("year"));
                    jsonObject.put("mark", video.get("mark"));
                }
            }

            String topicId = jsonObject.getString("topicId");
            if (!StringUtils.isEmpty(topicId)) {
                JSONObject videoTopic = broadcast.value().getVideoTopic(topicId);
                if (videoTopic != null) {
                    jsonObject.put("topicCid", videoTopic.get("cid"));
                    jsonObject.put("specId", videoTopic.get("specialType"));
                    jsonObject.put("topic", videoTopic.get("title"));
                    jsonObject.put("topicType", videoTopic.get("template"));
                }
            }

            Long rectime = jsonObject.getLong("rectime");
            if (rectime != null) {
                jsonObject.put("date", DateFormatUtils.format(rectime, "yyyyMMdd"));
            }
            return jsonObject.toJSONString();
        });
    }
}

