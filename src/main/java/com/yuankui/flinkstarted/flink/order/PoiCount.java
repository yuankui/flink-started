package com.yuankui.flinkstarted.flink.order;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class PoiCount {
    private int poiId;
    private long count;
}
