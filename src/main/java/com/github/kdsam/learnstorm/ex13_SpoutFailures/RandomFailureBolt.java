package com.github.kdsam.learnstorm.ex13_SpoutFailures;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class RandomFailureBolt extends BaseRichBolt {

    private static final Integer MAX_PERCENT_FAIL =  80;
    Random random = new Random();
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer r = random.nextInt(100);
        if (r < MAX_PERCENT_FAIL) {
            collector.emit(tuple, new Values(tuple.getString(0), tuple.getString(0), tuple.getString(1)));
            collector.ack(tuple);
        } else {
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("integer", "bucket"));
    }
}
