package net.myvst.v2.task;

import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.task.impl.HomeClassifyClickTask;
import net.myvst.v2.task.impl.UserRecordTask;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TaskChain {
    private static TaskChain instance = new TaskChain();
    private List<Task> taskList;

    private TaskChain() {
        taskList = new ArrayList<>();
        taskList.add(new HomeClassifyClickTask());
        taskList.add(new UserRecordTask());
    }

    public static TaskChain getInstance() {
        return instance;
    }

    public void process(JavaRDD<String> rdd) {
        for (Task task : taskList) {
            try {
                task.process(rdd);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
