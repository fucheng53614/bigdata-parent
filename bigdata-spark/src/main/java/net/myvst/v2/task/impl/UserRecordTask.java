package net.myvst.v2.task.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.bean.UserRecord;
import net.myvst.v2.db.ICommonDao;
import net.myvst.v2.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.spark.api.java.JavaRDD;
import org.spark_project.guava.collect.Lists;
import scala.Tuple2;

import java.sql.SQLException;
import java.util.*;

@Slf4j
public class UserRecordTask implements Task {

    @Override
    public Object process(JavaRDD<String> rdd) {
        return rdd.map(JSONObject::parseObject)
                .filter(json -> {
                    String action = json.getString("kafkaTopic");
                    return "user".equals(action) || "user_dayly".equals(action)
                            || "click".equals(action)
                            || "movie_click".equals(action)
                            || "movie_play".equals(action);
                }).mapToPair(jsonObject -> {
                    String pkg = String.valueOf(jsonObject.getString("pkg"));
                    String uuid = String.valueOf(jsonObject.getString("uuid"));
                    String channel = String.valueOf(jsonObject.getString("channel"));
                    String ip = String.valueOf(jsonObject.getString("ip"));
                    String bdModel = String.valueOf(jsonObject.getString("bdModel"));
                    String eth0Mac = String.valueOf(jsonObject.getString("eth0Mac"));
                    Long parseDpi = jsonObject.getLong("dpi");
                    long dpi = parseDpi == null ? -1 : parseDpi;
                    Long parseCpuCnt = jsonObject.getLong("cpuCnt");
                    long cpuCnt = parseCpuCnt == null ? -1 : parseCpuCnt;
                    String cpuName = String.valueOf(jsonObject.getString("cpuName"));
                    String country = String.valueOf(jsonObject.getString("country"));
                    String region = String.valueOf(jsonObject.getString("region"));
                    String city = String.valueOf(jsonObject.getString("city"));
                    boolean touch = String.valueOf(jsonObject.getBoolean("touch")).equals("true");
                    Long parseLargeMem = jsonObject.getLong("largeMem");
                    long largeMem = parseLargeMem == null ? -1 : parseLargeMem;
                    Long parseLimitMem = jsonObject.getLong("limitMem");
                    long limitMem = parseLimitMem == null ? -1 : parseLimitMem;
                    String screen = jsonObject.getString("screen");
                    String verCode = jsonObject.getString("verCode");
                    String verName = jsonObject.getString("verName");
                    boolean isVip = String.valueOf(jsonObject.getBoolean("isVip")).equals("true");
                    String bdCpu = jsonObject.getString("bdCpu");
                    String deviceName = jsonObject.getString("deviceName");
                    long firstTime = jsonObject.getLong("rectime");
                    long lastTime = jsonObject.getLong("rectime");
                    String activeDates = jsonObject.getString("date");
                    long click = "click".equals(jsonObject.getString("kafkaTopic")) ? 1 : 0;
                    long movieClick = "movie_click".equals(jsonObject.getString("kafkaTopic")) ? 1 : 0;
                    long moviePlay = "movie_play".equals(jsonObject.getString("kafkaTopic")) ? 1 : 0;
                    return new Tuple2<>(MD5Hash.digest(uuid).toString(), new UserRecord(pkg, uuid, channel, ip, bdModel, eth0Mac, dpi, cpuCnt, cpuName, country, region,
                            city, touch, largeMem, limitMem, screen, verCode, verName, isVip, bdCpu, deviceName, firstTime, lastTime, activeDates, click, movieClick, moviePlay));
                }).reduceByKey((u1, u2) -> {
                    long firstTime = Math.min(u1.getFirstTime(), u2.getFirstTime());
                    long lastTime = Math.max(u1.getFirstTime(), u2.getFirstTime());
                    u1.setFirstTime(firstTime);
                    u1.setLastTime(lastTime);
                    u1.setClick(u1.getClick() + u2.getClick());
                    u1.setMovieClick(u1.getMovieClick() + u2.getMovieClick());
                    u1.setMoviePlay(u1.getMoviePlay() + u2.getMoviePlay());
                    if (!StringUtils.isEmpty(u2.getActiveDates()) &&
                            !StringUtils.isEmpty(u1.getActiveDates())) {
                        if (!u1.getActiveDates().equals(u2.getActiveDates())) {
                            String[] u1SplitActiveDates = u1.getActiveDates().split(",");
                            String[] u2SplitActiveDates = u2.getActiveDates().split(",");

                            HashSet<String> activeDatesSet = Sets.newHashSet();
                            activeDatesSet.addAll(Arrays.asList(u1SplitActiveDates));
                            activeDatesSet.addAll(Arrays.asList(u2SplitActiveDates));

                            ArrayList<String> activeDatesList = Lists.newArrayList(activeDatesSet);
                            Collections.sort(activeDatesList);
                            u1.setActiveDates(StringUtils.join(activeDatesList, ","));
                        }
                    } else if (!StringUtils.isEmpty(u2.getActiveDates())) {
                        u1.setActiveDates(u2.getActiveDates());
                    }
                    return u1;
                }).collect();
    }

    @Override
    public String getTableName() {
        return "bigdata_bi.vst_user_record";
    }

    @Override
    public String createTableSql() {
        return "CREATE TABLE IF NOT EXISTS " + getTableName() + "(id VARCHAR PRIMARY KEY, pkg VARCHAR, uuid VARCHAR, channel VARCHAR, ip VARCHAR, bdModel VARCHAR, country VARCHAR, province VARCHAR, city VARCHAR, verCode VARCHAR, isVip BOOLEAN, firstTime BIGINT, lastTime BIGINT, activeDates VARCHAR, click BIGINT, movieClick BIGINT, moviePlay BIGINT)";
    }

    @Override
    public void store(ICommonDao db, Object obj) throws SQLException {
        List<Tuple2<String, UserRecord>> data = (List<Tuple2<String, UserRecord>>) obj;

        String sql = "UPSERT INTO " + getTableName() + "(id, pkg, uuid, channel, ip, bdModel, country, province, city, verCode, isVip, firstTime, lastTime, activeDates, click, movieClick, moviePlay) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE click=click+?,movieClick=movieClick+?,moviePlay=moviePlay+?,lastTime=?,activeDates=DISTINCT_CONCAT(activeDates,?),isVip=?";

        int size = data.size();
        Object[][] oo = new Object[size][];
        for (int i = 0; i < size; i++) {
            Tuple2<String, UserRecord> t2 = data.get(i);
            String key = t2._1;
            UserRecord value = t2._2;

            oo[i] = new Object[]{
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
                    value.getClick(),
                    value.getMovieClick(),
                    value.getMoviePlay(),

                    value.getClick(),
                    value.getMovieClick(),
                    value.getMoviePlay(),
                    value.getLastTime(),
                    "," + value.getActiveDates(),
                    value.isVip()
            };
        }
        int rows = db.insertBatch(sql, oo);
        log.info("commit [{}] rows [{}]", data.size(), rows);

    }
}
