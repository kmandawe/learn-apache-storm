package com.github.kdsam.learnstorm.ex23_Windowing;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class LogSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private static final Integer MAX_PERCENT_FAIL = 80;
    Random random = new Random();

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

        Integer r = random.nextInt(100);
        if (r < MAX_PERCENT_FAIL) {
            collector.emit(new Values("Success"));
        } else {
            collector.emit(new Values("ERROR"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
