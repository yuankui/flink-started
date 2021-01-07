package com.yuankui.flinkstarted.flink.order;

import lombok.Data;

@Data
public class Order {
    private int poiId;
    private int userId;
    private int orderId;
    private int price;
}
