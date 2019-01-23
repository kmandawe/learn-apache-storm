package com.github.kdsam.learnstorm.ex13_SpoutFailures;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntegerSpout extends BaseRichSpout {

    private static Integer MAX_FAILS = 3;
    private SpoutOutputCollector collector;
    private Map<Integer, Integer> integerFailureCount;
    private List<Integer> toSend;

    static Logger LOG = LoggerFactory.getLogger(IntegerSpout.class.getName());

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.toSend = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            toSend.add(i);
        }

        this.integerFailureCount = new HashMap<>();
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (!toSend.isEmpty()) {
            for (Integer current: toSend) {
                Integer intBucket = (current / 10);
                this.collector.emit(new Values(current.toString(), intBucket.toString()), current);
            }

            toSend.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("integer", "bucket"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println(msgId + " Successful");
    }

    @Override
    public void fail(Object msgId) {
        Integer failures = 1;

        Integer failedId = (Integer) msgId;

        if (integerFailureCount.containsKey(failedId)) {
            failures = integerFailureCount.get(failedId) + 1;
        }

        if (failures < MAX_FAILS) {
            integerFailureCount.put(failedId, failures);
            toSend.add(failedId);
            LOG.info("Re-sending message [" + failedId + "]");
        } else {
            LOG.info("Sending message [" + failedId + "] failed!");
        }
    }
}
