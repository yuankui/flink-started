package com.yuankui.flinkstarted.flink.order;

import com.alibaba.fastjson.JSON;
import com.yuankui.flinkstarted.flink.order.customize.ManualWindow;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Optional;
import java.util.Properties;

public class CountOrderManualWindow {
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
                .process(new ManualWindow<>(Time.seconds(60), Order.class))
                .map(elements -> {
                    Optional<Order> first = elements.stream().findFirst();

                    long count = elements.size();

                    long sum = elements.stream()
                            .mapToInt(Order::getPrice)
                            .sum();

                    return new PoiCount(first.get().getPoiId(), count, sum);
                })
                .addSink(new ElasticSink("http://some/index"));

        env.execute();
    }
}
