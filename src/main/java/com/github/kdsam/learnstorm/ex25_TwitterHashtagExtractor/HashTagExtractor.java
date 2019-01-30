package com.github.kdsam.learnstorm.ex25_TwitterHashtagExtractor;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashTagExtractor extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        // Get the tweet
        final Status status = (Status) tridentTuple.get(0);

        for (HashtagEntity hashtag: status.getHashtagEntities()) {
            tridentCollector.emit(new Values(hashtag.getText()));
        }
    }
}
