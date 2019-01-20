package com.github.kdsam.learnstorm.ex1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-First-Spout", new MyFirstSpout());
        builder.setBolt("My-First-Bolt", new MyFirstBolt()).shuffleGrouping("My-First-Spout");

        // Configuration
        Config conf = new Config();
        conf.setDebug(true);

        // Submit Topology to cluster
        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("My-First-Topology", conf, builder.createTopology());
            Thread.sleep(3000);
        } finally {
            cluster.shutdown();
        }
    }
}
