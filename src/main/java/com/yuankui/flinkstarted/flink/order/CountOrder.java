package com.yuankui.flinkstarted.flink.order;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.stream.StreamSupport;

public class CountOrder {
    public static void main(String[] args) throws Exception {
        // 订阅kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), properties));

        stream.map(line -> JSON.parseObject(line, Order.class))
                .keyBy(Order::getPoiId)
                .timeWindow(Time.seconds(60))
                .process(new ProcessWindowFunction<Order, PoiCount, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer poiId, Context context, Iterable<Order> elements, Collector<PoiCount> out) throws Exception {
                        long count = StreamSupport.stream(elements.spliterator(), false)
                                .count();

                        long sum = StreamSupport.stream(elements.spliterator(), false)
                                .mapToInt(Order::getPrice)
                                .sum();

                        out.collect(new PoiCount(poiId, count, sum));
                    }
                })
                .addSink(new ElasticSink("http://some/index"));

        env.execute();
    }
}
