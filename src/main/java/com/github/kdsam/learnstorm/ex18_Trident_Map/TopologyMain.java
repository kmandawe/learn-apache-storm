package com.github.kdsam.learnstorm.ex18_Trident_Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;

public class TopologyMain {

    public static void main(String[] args) {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple", drpc)
                .map(new LowerCase())
                .flatMap(new Split());

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology", conf, topology.build());

        for (String word: new String[]{"First Page", "Second Line", "Third Word in the Book"}) {
            System.out.println("Result for " + word + ": " + drpc.execute("simple", word));
        }

        cluster.shutdown();
    }
}
