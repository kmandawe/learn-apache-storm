package com.github.kdsam.learnstorm.ex11_WordReader;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt extends BaseBasicBolt {

    private Integer id;
    private Map<String, Integer> counters;
    private String name;
    private String fileName;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.fileName = stormConf.get("dirToWrite").toString() + "output" + "-"
                                + context.getThisTaskId()
                                + "-" + context.getThisComponentId()
                                +".txt";
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String str = tuple.getString(0);

        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
    }

    @Override
    public void cleanup() {
        try {
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            for (Map.Entry<String, Integer> entry: counters.entrySet()) {
                writer.println(entry.getKey() + ": " + entry.getValue());
            }
            writer.close();
        } catch (Exception e) {

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
