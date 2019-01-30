package com.github.kdsam.learnstorm.ex15_HDFSIntegration;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class FilterFieldsBolt extends BaseBasicBolt {

    public void cleanup() {

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String firstName = tuple.getStringByField("first_name");
        String lastName = tuple.getString(2);
        basicOutputCollector.emit(new Values(firstName, lastName));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("first_name", "last_name"));
    }

}
