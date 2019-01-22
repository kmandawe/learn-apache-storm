package com.github.kdsam.learnstorm.ex6_StreamGrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Integer-Spout", new IntegerSpout());
        builder.setBolt("Write-to-File-Bolt", new WriteToFileBolt(), 2).shuffleGrouping("Integer-Spout");

        // Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dirToWrite", "/Users/kmandawe/Projects/run/stormoutput/");

        // Topology run
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Shuffle-Grouping-Topology", conf, builder.createTopology());
            Thread.sleep(10000);
        } finally {
            cluster.shutdown();
        }
    }
}
