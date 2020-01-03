package net.myvst.v2.db;

import net.myvst.v2.manager.ConfigManager;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.KeyedHandler;
import org.apache.commons.dbutils.handlers.MapHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * JDBC常用操作
 * 1.查询，2.执行（新增/更新）,3. 批量新增
 * <p>
 * <p>
 * //新增或者更新、新增或者累加、新增累加替换
 */
public class DBOperator {
    private DataSource dataSource = null;
    private int batch;

    public DBOperator() {
        batch = ConfigManager.getInstance().getInt(ConfigManager.SAVE_BATCH);
        String prefix = ConfigManager.getInstance().getString(ConfigManager.SAVE_JDBC);
        Properties tmpPro = ConfigManager.getInstance().getSub2Properties(prefix + ".");

        Properties info = new Properties();
        for (Map.Entry<Object, Object> entry : tmpPro.entrySet()) {
            String key = String.valueOf(entry.getKey()).substring(prefix.length() + 1);
            info.put(key, entry.getValue());
        }
        try {
            dataSource = BasicDataSourceFactory.createDataSource(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void update(String sql, Object[] param) throws SQLException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        queryRunner.update(sql, param);
    }

    public void update(String sql) throws SQLException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        queryRunner.update(sql);
    }

    public Map<String, Map<String, Object>> query(String sql, String key) throws SQLException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        return (Map<String, Map<String, Object>>) queryRunner.query(sql, new KeyedHandler(key));
    }

    public Map<String, Object> queryOne(String sql) throws SQLException {
        QueryRunner queryRunner = new QueryRunner(dataSource);
        return queryRunner.query(sql, new MapHandler());
    }

    public void batch(String sql, Object[][] data, int batch) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            QueryRunner queryRunner = new QueryRunner();
            for (int i = 0; i < data.length; i++) {
                queryRunner.update(connection, sql, data[i]);
                if ((i + 1) % batch == 0) {
                    connection.commit();
                }
            }
            connection.commit();
            connection.setAutoCommit(true);
        }
    }

    public void batch(String sql, Object[][] data) throws SQLException {
        batch(sql, data, batch);
    }


    public static void main(String[] args) throws SQLException {
        DBOperator dbOperator = new DBOperator();
        Object[][] objects = {
//                {"c003ac4774a0bf53284577a6d934c4c2"}
                {"88c3fed11f37b0e2176a99382d4b8867","net.myvst.v2","20200103","4","来吧！冠军","2","2","1578046334521","1578046334521","2","2","1578046334521"}

        };
        System.out.println(objects[0].length);

//        '88c3fed11f37b0e2176a99382d4b8867','net.myvst.v2','20200103','4','来吧！冠军','2','2','1578046334521','1578046334521','2','2','1578046334521',UPSERT INTO bigdata_bi.vst_movie_classify_click(vst_mcc_id,vst_mcc_date,vst_mcc_cid,vst_mcc_name,vst_mcc_nameId,vst_mcc_uv,vst_mcc_amount,vst_mcc_addtime,vst_mcc_uptime) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE vst_mcc_uv=vst_mcc_uv+?,vst_mcc_amount=vst_mcc_amount+?,vst_mcc_uptime=?

        //CREATE TABLE IF NOT EXISTS bigdata_bi.vst_home_classify_click( id VARCHAR PRIMARY KEY, vst_hcc_date VARCHAR, vst_hcc_cid VARCHAR, vst_hcc_uv BIGINT, vst_hcc_amount BIGINT, vst_hcc_addtime BIGINT, vst_hcc_uptime BIGINT)

        dbOperator.batch("UPSERT INTO bigdata_bi.vst_movie_classify_click(vst_mcc_id,vst_mcc_date,vst_mcc_cid,vst_mcc_name,vst_mcc_nameId,vst_mcc_uv,vst_mcc_amount,vst_mcc_addtime,vst_mcc_uptime) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE vst_mcc_uv=vst_mcc_uv+?,vst_mcc_amount=vst_mcc_amount+?,vst_mcc_uptime=?", objects);
//        dbOperator.batch("UPSERT INTO bigdata_bi.vst_user_record(id, pkg, uuid, channel, ip, bdModel, country, province, city, verCode, isVip, firstTime, lastTime, activeDates, count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", objects);


//        '8988e72efaecd554152b3f3aa58fb45b','net.myvst.v2','e86b164d-25ae-4e38-b12f-636e972e29c2','toptech','112.39.10.132','MStar Android TV','中国','辽宁','朝阳','4050','false','1578045379263','1578045379263','20200103','1','1','1578045379263',',20200103',UPSERT INTO bigdata_bi.vst_user_record(id, pkg, uuid, channel, ip, bdModel, country, province, city, verCode, isVip, firstTime, lastTime, activeDates, count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE count=count+?,lastTime=?,activeDates=REGEXP_REPLACE(activeDates,'$','?')

    }
}