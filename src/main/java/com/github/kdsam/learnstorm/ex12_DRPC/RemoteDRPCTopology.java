package com.github.kdsam.learnstorm.ex12_DRPC;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.daemon.drpc;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.util.ArrayList;
import java.util.List;

public class RemoteDRPCTopology {

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("drc-plusTen");
        builder.addBolt(new PlusTenBolt());

        Config conf = new Config();
        List<String> drpcServers = new ArrayList<>();
        drpcServers.add("localhost");
        conf.put(Config.DRPC_SERVERS, drpcServers);
        conf.put(Config.DRPC_PORT, 3772);

        StormSubmitter.submitTopology("drpc-plusTen", conf, builder.createRemoteTopology());

    }
}
