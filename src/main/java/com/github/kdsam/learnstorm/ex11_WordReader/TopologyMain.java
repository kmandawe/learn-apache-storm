package com.github.kdsam.learnstorm.ex11_WordReader;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReaderSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounterBolt(), 2)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        // Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToRead", "/Users/kmandawe/Projects/run/storminput/wordcountsentence.txt");
        conf.put("dirToWrite", "/Users/kmandawe/Projects/run/stormoutput/ex11/");

        // Topology run
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("WordCounter-Topology", conf, builder.createTopology());
            Thread.sleep(30000);
        } finally {
            cluster.shutdown();
        }
    }
}
