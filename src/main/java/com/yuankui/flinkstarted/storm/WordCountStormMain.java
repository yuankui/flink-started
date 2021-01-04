package com.yuankui.flinkstarted.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountStormMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WordCountSpout(), 5);
        builder.setBolt("split", new SplitWordBolt(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
}