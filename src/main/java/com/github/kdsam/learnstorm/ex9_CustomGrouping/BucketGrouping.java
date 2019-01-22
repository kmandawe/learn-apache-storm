package com.github.kdsam.learnstorm.ex9_CustomGrouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BucketGrouping implements CustomStreamGrouping, Serializable {

    private List<Integer> targetTasks;

    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
    }

    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<Integer>();
        Integer boltNum = Integer.parseInt(list.get(1).toString()) % targetTasks.size();
        boltIds.add(targetTasks.get(boltNum));
        return boltIds;
    }


}
