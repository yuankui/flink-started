package com.yuankui.flinkstarted.flink.order.customize;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;

public class ManualWindow<T> extends KeyedProcessFunction<Integer, T, List<T>> {
    private transient ValueState<Long> windowStartTime;
    private transient ListState<T> listState;

    private final Class<T> clazz;
    private final long windowSize;

    public ManualWindow(Time time, Class<T> clazz) {
        this.windowSize = time.toMilliseconds();
        this.clazz = clazz;
    }

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>("state_name", clazz);
        ValueStateDescriptor<Long> valueState = new ValueStateDescriptor<>("valueState", Long.class);
        this.listState = getRuntimeContext().getListState(descriptor);
        this.windowStartTime = getRuntimeContext().getState(valueState);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<List<T>> out) throws Exception {
        if (ctx.timestamp() - windowStartTime.value() > windowSize) {
            out.collect(Lists.newArrayList(listState.get()));
            listState.clear();
        }

        listState.add(value);
    }
}
