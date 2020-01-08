package net.myvst.v2.task.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.bean.StatCounter;
import net.myvst.v2.db.ICommonDao;
import net.myvst.v2.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.sql.SQLException;
import java.util.*;

@Slf4j
public class MovieClassifyClickTask implements Task {

    @Override
    public Object process(JavaRDD<String> rdd) {
        JavaPairRDD<String, StatCounter> pairRDD = rdd.map(JSONObject::parseObject)
                .filter(j -> "net.myvst.v2".equals(j.getString("pkg")) && !StringUtils.isEmpty(j.getString("nameId")))
                .mapToPair(json -> {
                    String uuid = json.getString("uuid");
                    String pkg = json.getString("pkg");
                    String cid = json.getString("cid");
                    String date = json.getString("date");
                    String name = json.getString("name");
                    String nameId = json.getString("nameId");
                    String key = date + "[@!@]" + cid + "[@!@]" + name + "[@!@]" + nameId;
                    return new Tuple2<>(key, new StatCounter(1, Sets.newHashSet(uuid)));
                });

        List<Tuple2<String, StatCounter>> data = pairRDD.reduceByKey((statCounter1, statCounter2) -> {
            Set<String> key = new HashSet<>(statCounter1.getCountDistinct());
            key.addAll(statCounter2.getCountDistinct());
            statCounter1.setCount(statCounter1.getCount() + statCounter2.getCount());
            statCounter1.setCountDistinct(key);
            return statCounter1;
        }).collect();
        return data;
    }

    @Override
    public void store(ICommonDao db, Object obj) throws SQLException {
        List<Tuple2<String, StatCounter>> data = (List<Tuple2<String, StatCounter>>) obj;

        String insert = "UPSERT INTO " + getTableName() + "(vst_mcc_id,vst_mcc_date,vst_mcc_cid,vst_mcc_name,vst_mcc_nameId,vst_mcc_uv,vst_mcc_amount,vst_mcc_addtime,vst_mcc_uptime) " +
                "VALUES(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE vst_mcc_uv=vst_mcc_uv+?,vst_mcc_amount=vst_mcc_amount+?,vst_mcc_uptime=?";

        int size = data.size();
        Object[][] oo = new Object[size][];
        for (int i = 0; i < size; i++) {
            Tuple2<String, StatCounter> t2 = data.get(i);
            String key = t2._1;
            String[] idSplit = key.split("\\[@!@]");

            StatCounter value = t2._2;
            long uv = value.getCountDistinct().size();
            long vv = value.getCount();

            String md5Id = MD5Hash.digest(key).toString();
            long curTime = System.currentTimeMillis();

            Object[] objects = {md5Id, idSplit[0], idSplit[1], idSplit[2], idSplit[3], uv, vv, curTime, curTime, uv, vv, curTime};

            oo[i] = objects;
        }
        int rows = db.insertBatch(insert, oo);
        log.info("commit [{}] rows [{}]", data.size(), rows);
    }

    @Override
    public String getTableName() {
        return "bigdata_bi.vst_movie_classify_click";
    }

    @Override
    public String createTableSql() {
        return "CREATE TABLE IF NOT EXISTS " + getTableName() + "( vst_mcc_id VARCHAR PRIMARY KEY, vst_mcc_date VARCHAR, vst_mcc_cid VARCHAR, vst_mcc_name VARCHAR, vst_mcc_nameId VARCHAR, vst_mcc_uv BIGINT, vst_mcc_amount BIGINT, vst_mcc_addtime BIGINT, vst_mcc_uptime BIGINT)";
    }
}
