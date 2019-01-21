package com.github.kdsam.learnstorm.ex3;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterFieldsBolt extends BaseBasicBolt {

    public void cleanup() {}

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String firstName = tuple.getStringByField("first_name");
        String lastName = tuple.getString(2);
        basicOutputCollector.emit(new Values(firstName, lastName));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("first_name", "last_name"));
    }

}
