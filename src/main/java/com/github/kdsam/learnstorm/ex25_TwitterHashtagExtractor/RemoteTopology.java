package com.github.kdsam.learnstorm.ex25_TwitterHashtagExtractor;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

public class RemoteTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        final Config config = new Config();
        final IBatchSpout spout = new TwitterTridentSpout();
        config.setDebug(true);

        final TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .each(new Fields("tweet"), new HashTagExtractor(), new Fields("hashtag"))
                .groupBy(new Fields("hashtag"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .applyAssembly(new FirstN(10, "count"))
                .each(new Fields("hashtag", "count"), new Debug());

        // Build and return the topology
        StormSubmitter.submitTopology("hashtag-count-topology", config, topology.build());
    }
}
