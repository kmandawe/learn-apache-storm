package com.github.kdsam.learnstorm.ex5_stormCluster;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-First-Spout", new MyFirstSpout());
        builder.setBolt("My-First-Bolt", new MyFirstBolt()).shuffleGrouping("My-First-Spout");

        // Configuration
        Config conf = new Config();
        conf.setDebug(true);


        StormSubmitter.submitTopology("My-First-Topology", conf, builder.createTopology());

    }
}
