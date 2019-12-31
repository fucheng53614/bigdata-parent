package net.myvst.v2.task.impl;

import com.alibaba.fastjson.JSONObject;
import net.myvst.v2.bean.StatCounter;
import net.myvst.v2.db.DBPhoenixInstance;
import net.myvst.v2.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple6;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class HomeClassifyClickTask implements Task {
    private DBPhoenixInstance db = DBPhoenixInstance.getInstance();
    private final String table = "bigdata_bi.vst_home_classify_click";
    private final String create = "CREATE TABLE IF NOT EXISTS " + table + "( id VARCHAR PRIMARY KEY, date VARCHAR, cid VARCHAR, uv BIGINT, vv BIGINT, insertTime BIGINT)";

    public HomeClassifyClickTask() {
        Connection connection = null;
        try {
            connection = db.getConnection();
            db.executorUpdate(connection, create);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void process(JavaRDD<String> rdd) throws Exception {
        List<Tuple2<String, StatCounter>> data = rdd.map(line -> {
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
        }).reduceByKey((statCounter1, statCounter2) -> {
            Set<String> key = new HashSet<>(statCounter1.getCountDistinct());
            key.addAll(statCounter2.getCountDistinct());
            statCounter1.setCount(statCounter1.getCount() + statCounter2.getCount());
            statCounter1.setCountDistinct(key);
            return statCounter1;
        }).collect();

        store(data);
    }

    private void store(List<Tuple2<String, StatCounter>> data) throws SQLException {
        Connection connection = null;

        try {
            connection = db.getConnection();
            Map<String, Map<String, Object>> map = new HashMap<>();
            for (Tuple2<String, StatCounter> tuple2 : data) {
                String id = tuple2._1;
                String[] idSplit = id.split("\\[@!@]");
                StatCounter value = tuple2._2;
                long uv = value.getCountDistinct().size();
                long vv = value.getCount();

                Map<String, Object> mapValue = map.computeIfAbsent(id, v -> new HashMap<String, Object>());
                mapValue.put("id", id);
                mapValue.put("date", idSplit[0]);
                mapValue.put("cid", idSplit[1]);
                mapValue.put("uv", uv);
                mapValue.put("vv", vv);
                mapValue.put("insertTime", System.currentTimeMillis());
            }
            db.insertOrAccumulate(connection, table, "id", map, Arrays.asList("id", "date", "cid", "uv", "vv", "insertTime"), Arrays.asList("id", "uv", "vv"), 1000);
        }finally {
            if (connection != null) connection.close();
        }
    }
}
