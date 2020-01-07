package net.myvst.v2.task.impl;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.bean.StatCounter;
import net.myvst.v2.db.DBOperator;
import net.myvst.v2.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple6;

import java.sql.SQLException;
import java.util.*;

@Slf4j
public class HomeClassifyClickTask implements Task {

    @Override
    public String getTableName() {
        return "bigdata_bi.vst_home_classify_click";
    }

    @Override
    public String createTableSql() {
        return "CREATE TABLE IF NOT EXISTS " + getTableName() + "( id VARCHAR PRIMARY KEY, vst_hcc_date VARCHAR, vst_hcc_cid VARCHAR, vst_hcc_uv BIGINT, vst_hcc_amount BIGINT, vst_hcc_addtime BIGINT, vst_hcc_uptime BIGINT)";
    }

    @Override
    public Object process(JavaRDD<String> rdd) throws Exception {
        JavaPairRDD<String, StatCounter> pairRDD = rdd.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            String uuid = jsonObject.getString("uuid");
            String entry1 = jsonObject.getString("entry1");
            String date = jsonObject.getString("date");
            String cid = jsonObject.getString("cid");
            String pkg = jsonObject.getString("pkg");
            String action = jsonObject.getString("kafkaTopic");
            return new Tuple6<>(date, pkg, cid, uuid, entry1, action);
        }).filter(t6 -> {
            if (StringUtils.isEmpty(t6._5())) return false;
            String[] splitEntry1 = t6._5().split("\\|&");
            if (StringUtils.isEmpty(t6._3())
                    || !t6._2().equals("net.myvst.v2")
                    || splitEntry1.length != 2
                    || !splitEntry1[0].equals("精选")
                    || !"click".equals(t6._6())) return false;
            String[] cidContains = new String[]{"1", "2", "3", "4", "5", "8", "514", "525", "526", "529"};
            for (String cidContain : cidContains) if (t6._3().equals(cidContain)) return true;
            return false;
        }).mapToPair(t6 -> {
            String key = t6._1() + "[@!@]" + t6._3();
            return new Tuple2<>(key, new StatCounter(1, Collections.singleton(t6._4())));
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
    public void store(DBOperator db, Object obj) throws SQLException {
        List<Tuple2<String, StatCounter>> data = (List<Tuple2<String, StatCounter>>) obj;

        String insert = "UPSERT INTO " + getTableName() + "(id,vst_hcc_date,vst_hcc_cid,vst_hcc_uv,vst_hcc_amount,vst_hcc_addtime,vst_hcc_uptime) " +
                "VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE vst_hcc_uv=vst_hcc_uv+?,vst_hcc_amount=vst_hcc_amount+?,vst_hcc_uptime=?";

        int size = data.size();
        Object[][] oo = new Object[size][];
        for (int i = 0; i < size; i++) {
            Tuple2<String, StatCounter> t2 = data.get(i);
            String key = t2._1;
            String[] idSplit = key.split("\\[@!@]");
            String date = idSplit[0];
            String cid = idSplit[1];

            StatCounter value = t2._2;
            long uv = value.getCountDistinct().size();
            long vv = value.getCount();

            String md5Id = MD5Hash.digest(key).toString();

            long curTime = System.currentTimeMillis();
            Object[] objects = new Object[]{md5Id, date, cid, uv, vv, curTime, curTime, uv, vv, curTime};
            oo[i]= objects;
        }
        int rows = db.batch(insert, oo);
        log.info("commit [{}] rows [{}]", data.size(), rows);
    }
}
