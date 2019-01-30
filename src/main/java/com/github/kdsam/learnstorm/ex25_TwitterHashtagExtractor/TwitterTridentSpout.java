package com.github.kdsam.learnstorm.ex25_TwitterHashtagExtractor;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterTridentSpout implements IBatchSpout {

    private TwitterStream twitterStream;
    private Queue<Status> queue;

    @Override
    public void open(Map map, TopologyContext topologyContext) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("WdaVx2NmB3NeYB9yxcygYgADj")
                .setOAuthConsumerSecret("w5c3GFINR6Orkvowwa3IQTTJjuGB2iUjzYdY2es3gfw3YoilXR")
                .setOAuthAccessToken("1086128348105850880-X09awwcxmOJQls5Tk7woOObI8r6zMr")
                .setOAuthAccessTokenSecret("9W9fRYFPLr5zm9B776JSQK5Pi0PhoMa4S4ghvr8j0PdOb");

        this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        this.queue = new LinkedBlockingQueue<>();

        final StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };

        twitterStream.addListener(listener);
        final FilterQuery query = new FilterQuery();
        query.track(new String[]{"chocolate"});
        twitterStream.filter(query);
    }

    @Override
    public void emitBatch(long l, TridentCollector tridentCollector) {
        final Status status = queue.poll();

        if (status == null) {
            Utils.sleep(50);
        } else {
            tridentCollector.emit(new Values(status));
        }
    }

    @Override
    public void ack(long l) {

    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tweet");
    }
}
