package com.github.kdsam.learnstorm.ex10_DirectGrouping;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class DirectGroupingSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Integer i = 0;
    private List<Integer> boltIds;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.boltIds = topologyContext.getComponentTasks("Write-to-File-Bolt");
    }

    public void nextTuple() {

        while (i <= 100) {
            Integer intBucket = (this.i/10);

            this.collector.emitDirect(boltIds.get(getBoltId(intBucket)),
                    new Values(this.i.toString(), intBucket.toString()));
            this.i = this.i + 1;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("integer", "bucket"));
    }

    private int getBoltId(Integer intBucket) {
        return intBucket % boltIds.size();
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }
}
