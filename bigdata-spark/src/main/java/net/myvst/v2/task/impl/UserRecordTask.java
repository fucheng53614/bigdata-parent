package net.myvst.v2.task.impl;

import com.alibaba.fastjson.JSONObject;
import net.myvst.v2.bean.UserRecord;
import net.myvst.v2.db.DBPhoenixInstance;
import net.myvst.v2.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class UserRecordTask implements Task {
    private DBPhoenixInstance db = DBPhoenixInstance.getInstance();

    private final String table = "bigdata_bi.vst_user_record";
    private final String create = "CREATE TABLE IF NOT EXISTS " + table + "( id VARCHAR PRIMARY KEY, pkg VARCHAR, uuid VARCHAR, channel VARCHAR, ip VARCHAR,bdModel VARCHAR,eth0Mac VARCHAR,dpi BIGINT,cpuCnt BIGINT,cpuName VARCHAR,country VARCHAR,region VARCHAR,city VARCHAR,touch BOOLEAN,largeMem BIGINT,limitMem BIGINT,screen VARCHAR,verCode VARCHAR,verName VARCHAR,isVip BOOLEAN,bdCpu VARCHAR,deviceName VARCHAR,firstTime BIGINT,lastTime BIGINT,activeDates VARCHAR, count BIGINT)";

    public UserRecordTask() {
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
        List<Tuple2<String, UserRecord>> data = rdd.map(JSONObject::parseObject).filter(jsonObject -> {
            String action = jsonObject.getString("kafkaTopic");
            return action.equals("user") || action.equals("user_dayly") || action.equals("qq_user");
        }).mapToPair(jsonObject -> {
            String pkg = jsonObject.getString("pkg");
            String uuid = jsonObject.getString("uuid");
            String channel = jsonObject.getString("channel");
            String ip = jsonObject.getString("ip");
            String bdModel = jsonObject.getString("bdModel");
            String eth0Mac = jsonObject.getString("eth0Mac");
            Long parseDpi = jsonObject.getLong("dpi");
            long dpi = parseDpi == null ? -1 : parseDpi;
            Long parseCpuCnt = jsonObject.getLong("cpuCnt");
            long cpuCnt = parseCpuCnt == null ? -1: parseCpuCnt;
            String cpuName = jsonObject.getString("cpuName");
            String country = jsonObject.getString("country");
            String region = jsonObject.getString("region");
            String city = jsonObject.getString("city");
            boolean touch = String.valueOf(jsonObject.getBoolean("touch")).equals("true");
            long largeMem = jsonObject.getLong("largeMem");
            long limitMem = jsonObject.getLong("limitMem");
            String screen = jsonObject.getString("screen");
            String verCode = jsonObject.getString("verCode");
            String verName = jsonObject.getString("verName");
            boolean isVip = String.valueOf(jsonObject.getBoolean("isVip")).equals("true");
            String bdCpu = jsonObject.getString("bdCpu");
            String deviceName = jsonObject.getString("deviceName");
            long firstTime = jsonObject.getLong("rectime");
            long lastTime = jsonObject.getLong("rectime");
            String activeDates = jsonObject.getString("date");
            return new Tuple2<>(MD5Hash.digest(uuid).toString(), new UserRecord(pkg, uuid, channel, ip, bdModel, eth0Mac, dpi, cpuCnt, cpuName, country, region,
                    city, touch, largeMem, limitMem, screen, verCode, verName, isVip, bdCpu, deviceName, firstTime, lastTime, activeDates, 1));
        }).reduceByKey((u1, u2) -> {
            long firstTime = Math.min(u1.getFirstTime(), u2.getFirstTime());
            long lastTime = Math.max(u1.getFirstTime(), u2.getFirstTime());
            u1.setFirstTime(firstTime);
            u1.setLastTime(lastTime);
            u1.setCount(u1.getCount() + u2.getCount());
            if (!StringUtils.isEmpty(u2.getActiveDates()) && !StringUtils.isEmpty(u1.getActiveDates())) {
                if (!u1.getActiveDates().equals(u2.getActiveDates())) {
                    if (Long.parseLong(u1.getActiveDates()) < Long.parseLong(u2.getActiveDates())) {
                        u1.setActiveDates(u1.getActiveDates() + "," + u2.getActiveDates());
                    } else {
                        u1.setActiveDates(u2.getActiveDates() + "," + u1.getActiveDates());
                    }
                }
            } else if (!StringUtils.isEmpty(u2.getActiveDates())) {
                u1.setActiveDates(u2.getActiveDates());
            }
            return u1;
        }).collect();

        store(data);
    }

    private void store(List<Tuple2<String, UserRecord>> data) throws SQLException {
        Connection connection = null;
        try {
            connection = db.getConnection();
            Map<String, Map<String, Object>> map = new HashMap<>();
            for (Tuple2<String, UserRecord> tuple2 : data) {
                String id = tuple2._1;
                UserRecord value = tuple2._2;

                Map<String, Object> mapValue = map.computeIfAbsent(id, v -> new HashMap<String, Object>());
                mapValue.put("id", value.getUuid());
                mapValue.put("pkg", value.getPkg());
                mapValue.put("uuid", value.getUuid());
                mapValue.put("channel", value.getChannel());
                mapValue.put("ip", value.getIp());
                mapValue.put("bdModel", value.getBdModel());
                mapValue.put("eth0Mac", value.getEth0Mac());
                mapValue.put("dpi", value.getDpi());
                mapValue.put("cpuCnt", value.getCpuCnt());
                mapValue.put("cpuName", value.getCpuName());
                mapValue.put("country", value.getCountry());
                mapValue.put("region", value.getRegion());
                mapValue.put("city", value.getCity());
                mapValue.put("touch", value.isTouch());
                mapValue.put("largeMem", value.getLargeMem());
                mapValue.put("limitMem", value.getLimitMem());
                mapValue.put("screen", value.getScreen());
                mapValue.put("verCode", value.getVerCode());
                mapValue.put("verName", value.getVerName());
                mapValue.put("isVip", value.isVip());
                mapValue.put("bdCpu", value.getBdCpu());
                mapValue.put("deviceName", value.getDeviceName());
                mapValue.put("firstTime", value.getFirstTime());
                mapValue.put("lastTime", value.getLastTime());
                mapValue.put("activeDates", value.getActiveDates());
                mapValue.put("count", value.getCount());
            }
            db.insertOrAccumulate(connection, table, "id", map,
                    Arrays.asList("id", "pkg", "uuid", "channel", "ip", "bdModel", "eth0Mac", "dpi", "cpuCnt", "cpuName", "country", "region", "city", "touch", "largeMem", "limitMem", "screen", "verCode", "verName", "isVip", "bdCpu", "deviceName", "firstTime", "lastTime", "activeDates", "count"),
                    Arrays.asList("id", "lastTime", "activeDates", "count"), 1000);
        } finally {
            if (connection != null) connection.close();
        }
    }
}
