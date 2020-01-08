package net.myvst.v2.config.impl;

import net.myvst.v2.config.AbstractConfig;
import net.myvst.v2.config.ITaskConfig;
import net.myvst.v2.task.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TaskConfigImpl extends AbstractConfig implements ITaskConfig {

    public TaskConfigImpl() {
        super("/task.properties");
    }

    @Override
    public Collection<Task> getTasks() {
        String tasks = properties.getProperty("tasks");
        if (tasks == null){
            throw new IllegalArgumentException("task.properties tasks param not config.");
        }
        String[] splitTasks = tasks.split(",");
        List<Task> taskList = new ArrayList<>(splitTasks.length);

        try {
            for (String task : splitTasks) {
                taskList.add((Task)Class.forName(task).newInstance());
            }
            return taskList;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
