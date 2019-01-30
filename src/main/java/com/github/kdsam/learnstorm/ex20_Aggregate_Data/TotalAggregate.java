package com.github.kdsam.learnstorm.ex20_Aggregate_Data;

import com.github.kdsam.learnstorm.ex18_Trident_Map.LowerCase;
import com.github.kdsam.learnstorm.ex18_Trident_Map.Split;
import com.github.kdsam.learnstorm.ex19_Filter.FilterShort;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class TotalAggregate {

    public static void main(String[] args) {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple", drpc)
                .map(new LowerCase())
                .flatMap(new Split())
                .aggregate(new Count(), new Fields("count"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology", conf, topology.build());

        for (String word: new String[]{"this is", "a very very short short book", "sentence in a book"}) {
            System.out.println("Result for " + word + ": " + drpc.execute("simple", word));
        }

        cluster.shutdown();
    }
}
