package net.myvst.v2.task;

import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.db.DBOperator;
import net.myvst.v2.utils.ConfigManager;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
public class TaskChain {
    private List<Task> taskList;

    public void init(DBOperator db) throws Exception {
        Collection<String> subValueList = ConfigManager.getProperties2List(ConfigManager.Config.TASK, "tasks");
        taskList = new ArrayList<>();
        for (String clz : subValueList) {
            Task task = (Task) Class.forName(clz).newInstance();
            db.update(task.createTableSql());
            taskList.add(task);
        }
    }

    public void process(DBOperator db, JavaRDD<String> rdd) throws Exception {
        log.info("total task size [{}]", taskList.size());
        for (int i = 0, size = taskList.size(); i < size; i++) {
            Task task = taskList.get(i);
            log.info("exec [{}]/[{}] [{}]", i + 1, size, task.getClass());
            Object process = task.process(rdd);
            task.store(db, process);
        }
    }
}
