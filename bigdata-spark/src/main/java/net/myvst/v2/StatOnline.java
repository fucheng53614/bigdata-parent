package net.myvst.v2;


import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.config.*;
import net.myvst.v2.config.impl.*;
import net.myvst.v2.db.ICommonDao;
import net.myvst.v2.db.impl.PhoenixDaoImpl;
import net.myvst.v2.manager.AbstractKafkaStreamingService;
import net.myvst.v2.manager.impl.KafkaStreamingServiceImpl;
import net.myvst.v2.task.TaskChain;

/**
 * 在线处理任务
 * /home/hadoop/spark/bin/spark-submit --conf "spark.driver.extraJavaOptions=-Dhbase.tmp.dir=hdfs://fuch0:9000" --master spark://fuch0:7077 --class net.myvst.v2.StatOnline bigdata-spark-1.0-SNAPSHOT.jar
 */
@Slf4j
public class StatOnline {

    private AbstractKafkaStreamingService abstractKafkaStreamingService;

    public StatOnline(){
        log.info("load config");
        IDBConfig dbConfig = new DBConfigImpl();
        ITaskConfig taskConfig = new TaskConfigImpl();
        ISparkConfig sparkConfig = new SparkConfigImpl();
        IKafkaConfig kafkaConfig = new KafkaConfigImpl();
        IAppConfig appConfig = new AppConfigImpl();

        log.info("init task");
        ICommonDao dao = new PhoenixDaoImpl(dbConfig);
        TaskChain chain = new TaskChain(dao, taskConfig.getTasks());
        abstractKafkaStreamingService = new KafkaStreamingServiceImpl(appConfig, sparkConfig, kafkaConfig, dao, chain);
    }

    public void process() throws Exception {
        log.info("start.");
        abstractKafkaStreamingService.online();
    }

    public static void main(String[] args) throws Exception {
        StatOnline statOnline = new StatOnline();
        statOnline.process();
    }
}

