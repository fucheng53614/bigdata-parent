package net.myvst.v2.manager;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.cache.IVideoAndTopicCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;


@Slf4j
public abstract class AbstractKafkaStreamingService {

    public abstract JavaStreamingContext createJavaStreamingContext();

    public abstract JavaInputDStream createKafkaDStream(JavaStreamingContext javaStreamingContext);

    public abstract IVideoAndTopicCache getVideoAndTopic();

    public void online() throws Exception {
        JavaStreamingContext ssc = createJavaStreamingContext();
        final Broadcast<IVideoAndTopicCache> broadcast = ssc.sparkContext().broadcast(getVideoAndTopic());
        JavaDStream<ConsumerRecord<String, String>> directStream = createKafkaDStream(ssc);
        directStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            JavaRDD<String> preProcessRDD = format(rdd.map(ConsumerRecord::value), broadcast);
            preProcessRDD.cache();

            try{
                process(preProcessRDD, offsetRanges);
            }finally {
                preProcessRDD.unpersist();
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }

    public abstract void process(JavaRDD<String> rdd, OffsetRange[] offsetRanges);

    private static JavaRDD<String> format(JavaRDD<String> rdd, Broadcast<IVideoAndTopicCache> broadcast) {
        return rdd.map(s -> {
            try {
                JSONObject jsonObject = JSONObject.parseObject(s);

                String nameId = jsonObject.getString("nameId");

                String kafkaTopic = jsonObject.getString("kafkaTopic");
                //如果是click事件
                if (!StringUtils.isEmpty(kafkaTopic) && "click".equals(kafkaTopic)){
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
                if (rectime == null){
                    rectime = System.currentTimeMillis();
                }
                jsonObject.put("date", DateFormatUtils.format(rectime, "yyyyMMdd"));

                return jsonObject.toJSONString();
            }catch (com.alibaba.fastjson.JSONException e){
                return "{}";
            }
        });
    }
}


