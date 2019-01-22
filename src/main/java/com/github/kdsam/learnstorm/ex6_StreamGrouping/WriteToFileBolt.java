package com.github.kdsam.learnstorm.ex6_StreamGrouping;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class WriteToFileBolt extends BaseBasicBolt {

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
        String str = tuple.getStringByField("integer") + "-" + tuple.getStringByField("bucket");
        basicOutputCollector.emit(new Values(str));
        writer.println(str);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }
}
