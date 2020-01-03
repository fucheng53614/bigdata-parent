package net.myvst.v2;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.bean.StatCounter;
import net.myvst.v2.bean.UserRecord;
import net.myvst.v2.cache.VideoAndTopicCache;

import net.myvst.v2.db.DBOperator;
import net.myvst.v2.manager.ConfigManager;
import net.myvst.v2.manager.OffsetManagerInterface;
import net.myvst.v2.task.TaskChain;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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

import static net.myvst.v2.manager.ConfigManager.KAFKA_TOPICS;
import static net.myvst.v2.manager.ConfigManager.SPARK_STREAMING_SECONDS;

/**
 * 在线处理任务
 */
@Slf4j
public class StatOnline {

    private static DBOperator dbOperator = new DBOperator();
    private static TaskChain taskChain = new TaskChain(dbOperator);
    private static OffsetManagerInterface offsetManager;

    static {
        Logger.getLogger("org").setLevel(Level.WARN);
        try {
            offsetManager = (OffsetManagerInterface) Class.forName(ConfigManager.getInstance().getString(ConfigManager.SYSTEM_OFFSET_MANAGER)).newInstance();
            dbOperator.update(offsetManager.createTableSql());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<>();
        String prefix = "kafka.";
        Properties kafka = ConfigManager.getInstance().getSub2Properties(prefix);
        for (Map.Entry<Object, Object> entry : kafka.entrySet()) {
            String key = (String) entry.getKey();
            if ((KAFKA_TOPICS).equals(key)) continue;
            kafkaParams.put(key.substring(prefix.length()), entry.getValue());
        }
        return kafkaParams;
    }

    private static SparkConf getSparkConf(){
        SparkConf conf = new SparkConf();
        String prefix = "spark.";
        Properties spark = ConfigManager.getInstance().getSub2Properties(prefix);
        for (Map.Entry<Object, Object> entry : spark.entrySet()) {
            String key = (String) entry.getKey();
            if (SPARK_STREAMING_SECONDS.equals(key)) continue;
            conf.set(key, String.valueOf(entry.getValue()));
        }
        conf.registerKryoClasses(new Class[]{Object.class, StatCounter.class, JSONObject.class, UserRecord.class, Set.class});
        return conf;
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        Collection<String> topics = ConfigManager.getInstance().getList(KAFKA_TOPICS);

        SparkConf conf = getSparkConf();
        Map<String, Object> kafkaParams = getKafkaParams();
        Map<TopicPartition, Long> topicPartitionMap = offsetManager.loadOffset(dbOperator, topics);

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(ConfigManager.getInstance().getLong(SPARK_STREAMING_SECONDS)));
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
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            JavaRDD<String> preProcessRDD = preProcess(rdd.map(ConsumerRecord::value), broadcast);
            preProcessRDD.cache();

            taskChain.process(preProcessRDD);

            preProcessRDD.unpersist();
            offsetManager.store(dbOperator, offsetRanges);
        });
        ssc.start();
        ssc.awaitTermination();

    }

    private static JavaRDD<String> preProcess(JavaRDD<String> rdd, Broadcast<VideoAndTopicCache> broadcast) {
        return rdd.map(s -> {
            JSONObject jsonObject = JSONObject.parseObject(s);

            String nameId = jsonObject.getString("nameId");

            //如果是click事件
            if ("click".equals(jsonObject.getString("kafkaTopic"))){
                String entry1Id = jsonObject.getString("entry1Id");
                if (!StringUtils.isEmpty(entry1Id)) {
                    String[] splitEntry1Id = entry1Id.split("\\|&");
                    nameId = splitEntry1Id[splitEntry1Id.length - 1];
                }
            }

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
                    jsonObject.put("nameId", nameId);
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

