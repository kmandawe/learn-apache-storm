package com.github.kdsam.learnstorm.ex23_Windowing;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.SlidingCountWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;

public class WindowTopology {

    public static void main(String[] args) throws InterruptedException {

        WindowsStoreFactory windowsStore = new InMemoryWindowsStoreFactory();

        WindowConfig windowConfig = SlidingCountWindow.of(100, 10);
        TridentTopology topology = new TridentTopology();

        topology.newStream("spout1", new LogSpout())
                .window(windowConfig, windowsStore, new Fields("log"),
                        new ErrorAggregator(), new Fields("count"))
                .each(new Fields("count"), new Debug());

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("trident-topology", conf, topology.build());
            Thread.sleep(15000);
        } finally {
            cluster.shutdown();
        }
    }
}
