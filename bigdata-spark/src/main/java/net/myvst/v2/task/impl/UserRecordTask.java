package net.myvst.v2.task.impl;

import com.alibaba.fastjson.JSONObject;
import net.myvst.v2.bean.UserRecord;
import net.myvst.v2.db.DBOperator;
import net.myvst.v2.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.sql.SQLException;
import java.util.*;

public class UserRecordTask implements Task {

    @Override
    public Object process(JavaRDD<String> rdd) {
        List<Tuple2<String, UserRecord>> data = rdd.map(JSONObject::parseObject)
                .mapToPair(jsonObject -> {
                    String pkg = jsonObject.getString("pkg");
                    String uuid = jsonObject.getString("uuid");
                    String channel = jsonObject.getString("channel");
                    String ip = jsonObject.getString("ip");
                    String bdModel = jsonObject.getString("bdModel");
                    String eth0Mac = jsonObject.getString("eth0Mac");
                    Long parseDpi = jsonObject.getLong("dpi");
                    long dpi = parseDpi == null ? -1 : parseDpi;
                    Long parseCpuCnt = jsonObject.getLong("cpuCnt");
                    long cpuCnt = parseCpuCnt == null ? -1 : parseCpuCnt;
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
        return data;
    }

    @Override
    public String getTableName() {
        return "bigdata_bi.vst_user_record";
    }

    @Override
    public String createTableSql() {
        return "CREATE TABLE IF NOT EXISTS " + getTableName() + "(id VARCHAR PRIMARY KEY, pkg VARCHAR, uuid VARCHAR, channel VARCHAR, ip VARCHAR, bdModel VARCHAR, country VARCHAR, province VARCHAR, city VARCHAR, verCode VARCHAR, isVip BOOLEAN, firstTime BIGINT, lastTime BIGINT, activeDates VARCHAR, count BIGINT)";
    }

    @Override
    public void store(DBOperator db, Object obj) throws SQLException {
        List<Tuple2<String, UserRecord>> data = (List<Tuple2<String, UserRecord>>) obj;
        Object[][] objects = new Object[data.size()][];
        for (int i = 0; i < data.size(); i++) {
            Tuple2<String, UserRecord> t2 = data.get(i);
            String key = t2._1;
            UserRecord value = t2._2;

            objects[i] = new Object[]{
                    key,
                    value.getPkg(),
                    value.getUuid(),
                    value.getChannel(),
                    value.getIp(),
                    value.getBdModel(),
                    value.getCountry(),
                    value.getRegion(),
                    value.getCity(),
                    value.getVerCode(),
                    value.isVip(),
                    value.getFirstTime(),
                    value.getLastTime(),
                    value.getActiveDates(),
                    value.getCount(),

                    value.getCount(),
                    value.getLastTime(),
                    "," + value.getActiveDates(),
                    value.isVip()
            };
        }
        String sql = "UPSERT INTO " + getTableName() + "(id, pkg, uuid, channel, ip, bdModel, country, province, city, verCode, isVip, firstTime, lastTime, activeDates, count) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE count=count+?,lastTime=?,activeDates=REGEXP_REPLACE(activeDates,'$',?),isVip=?";

        db.batch(sql, objects, 5000);
    }
}
