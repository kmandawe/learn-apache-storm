package com.github.kdsam.learnstorm.ex3;

import com.github.kdsam.learnstorm.ex2.FileReaderSpout;
import com.github.kdsam.learnstorm.ex2.SimpleBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Read-Fields-Spout", new ReadFieldsSpout());
        builder.setBolt("Filter-Fields-Bolt", new FilterFieldsBolt()).shuffleGrouping("Read-Fields-Spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToRead", "/Users/kmandawe/Desktop/fields.txt");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Read-Fields-Topology", conf, builder.createTopology());
            Thread.sleep(3000);
        } finally {
            cluster.shutdown();
        }
    }
}
