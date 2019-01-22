package com.github.kdsam.learnstorm.ex12_DRPC;

import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class DrpcClient {

    public static void main(String[] args) throws TException {
        Map conf = Utils.readStormConfig();
        DRPCClient client = new DRPCClient(conf, "localhost", 3772);

        for (Integer number: new Integer[]{53, 62, 70}) {
            System.out.println("Result for " + number + ": " + client.execute("drpc-plusTen", number.toString()));
        }
    }
}
