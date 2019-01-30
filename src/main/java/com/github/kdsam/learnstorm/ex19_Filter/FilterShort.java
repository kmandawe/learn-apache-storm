package com.github.kdsam.learnstorm.ex19_Filter;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class FilterShort extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return tridentTuple.getString(0).length() > 3;
    }
}
