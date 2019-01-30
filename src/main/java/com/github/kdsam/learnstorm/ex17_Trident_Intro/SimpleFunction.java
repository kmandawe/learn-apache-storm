package com.github.kdsam.learnstorm.ex17_Trident_Intro;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SimpleFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(tridentTuple.getString(0) + "After Processing"));
    }

}
