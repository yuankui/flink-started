package com.yuankui.flinkstarted.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkWordCount {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordWithCount {
        public String word;
        public long count;
    }

    public static void main(String[] args) throws Exception {
        // 1. 解析外部参数,获取要监听的主机、端口,没有配置则取默认值localhost:9999
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "localhost");
        int port = tool.getInt("port", 9999);

        // 2. 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. 初始化数据
        DataStreamSource<String> source = env.socketTextStream(host, port);

        // 4. 计算,扁平化,每个单次计数为1,分组,累加次数
        SingleOutputStreamOperator<WordWithCount> counts = source
                .flatMap((line, collector) -> {
                    String[] words = line.split("\\s+");
                    for (String word : words) {
                        collector.collect(new WordWithCount(word, 1L));
                    }
                }, TypeInformation.of(WordWithCount.class))
                .keyBy(wc -> wc.word)
                .timeWindow(Time.seconds(3))
                .reduce((w1, w2) -> new WordWithCount(w1.getWord(), w1.getCount() + w2.getCount()));

        // 5. 打印结果,设置并行度
        counts.print().setParallelism(1);

        // 6. 开启流任务,这是一个action算子,将触发计算
        env.execute("SocketWindowWordCountJava");
    }
}
