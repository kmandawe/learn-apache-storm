package com.github.kdsam.learnstorm.ex10_DirectGrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Integer-Spout", new DirectGroupingSpout());
        builder.setBolt("Write-to-File-Bolt", new WriteToFileBolt(), 2)
                        .directGrouping("Integer-Spout");

        // Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "/Users/kmandawe/Projects/run/stormoutput/");

        // Topology run
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Direct-Grouping-Topology", conf, builder.createTopology());
            Thread.sleep(10000);
        } finally {
            cluster.shutdown();
        }
    }
}
