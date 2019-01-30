package com.github.kdsam.learnstorm.ex17_Trident_Intro;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

    public static void main(String[] args) {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple", drpc)
                .each(new Fields("args"),
                        new SimpleFunction(),
                        new Fields("processed_word"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology", conf, topology.build());

        for (String word: new String[]{"word 1", "word 2", "word 3"}) {
            System.out.println("Result for " + word + ": " + drpc.execute("simple", word));
        }

        cluster.shutdown();
    }
}
