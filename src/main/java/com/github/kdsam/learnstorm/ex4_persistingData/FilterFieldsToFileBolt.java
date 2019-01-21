package com.github.kdsam.learnstorm.ex4_persistingData;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class FilterFieldsToFileBolt extends BaseBasicBolt {

    private PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String fileName = "output" + "-" + context.getThisTaskId() + "-"
                + context.getThisComponentId() + ".txt";

        try {
            this.writer = new PrintWriter(
                    stormConf.get("dirToWrite").toString() + fileName,
                    "UTF-8");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void cleanup() {
        writer.close();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String firstName = tuple.getStringByField("first_name");
        String lastName = tuple.getString(2);
        writer.println(firstName + "," + lastName);
        basicOutputCollector.emit(new Values(firstName, lastName));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("first_name", "last_name"));
    }

}
