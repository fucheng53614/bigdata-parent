package net.myvst.v2.task;

import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.db.DBOperator;
import net.myvst.v2.manager.ConfigManager;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
public class TaskChain {
    protected DBOperator db;
    private List<Task> taskList;

    public TaskChain(DBOperator db) {
        this.db = db;
        Collection<Object> subValueList = ConfigManager.getInstance().getSub2Properties(ConfigManager.SYSTEM_TASKS_SUB).values();
        taskList = new ArrayList<>();
        for (Object clz : subValueList) {
            try {
                Task task = (Task) Class.forName(String.valueOf(clz)).newInstance();
                db.update(task.createTableSql());
                taskList.add(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void process(JavaRDD<String> rdd) throws Exception {
        for (Task task : taskList) {
            task.store(db, task.process(rdd));
        }
    }
}
