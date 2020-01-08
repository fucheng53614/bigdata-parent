package net.myvst.v2.task;

import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.db.ICommonDao;
import org.apache.spark.api.java.JavaRDD;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

@Slf4j
public class TaskChain {
    private Collection<Task> taskList;
    private ICommonDao db;
    private int taskSize;

    public TaskChain(ICommonDao db, Collection<Task> taskList) {
        this.db = db;
        this.taskList = taskList;

        for (Task task : taskList) {
            try {
                db.create(task.createTableSql());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        this.taskSize = taskList.size();
    }

    public void process(JavaRDD<String> rdd) throws Exception {
        log.info("total task size [{}]", taskSize);
        Iterator<Task> iterator = taskList.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            Task task = iterator.next();
            Object process = task.process(rdd);
            task.store(db, process);
            log.info("exec [{}]/[{}] [{}]", i + 1, taskSize, task.getClass().getName());
            i++;
        }
    }
}
