package com.yuankui.flinkstarted.flink.order;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElasticSink extends AbstractRichFunction implements SinkFunction<PoiCount> {
    private String url;
    @Override
    public void invoke(PoiCount value, Context context) throws Exception {
        // write to es
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // init es client
    }
}
