package com.github.kdsam.learnstorm.ex2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderSpout());
        builder.setBolt("Simple-Bolt", new SimpleBolt()).shuffleGrouping("File-Reader-Spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToRead", "/Users/kmandawe/Desktop/sample.txt");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("File-Reader-Topology", conf, builder.createTopology());
            Thread.sleep(3000);
        } finally {
            cluster.shutdown();
        }
    }
}
