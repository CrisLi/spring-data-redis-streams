package org.chris.demo.redis.streams;

import java.util.Map;

import lombok.Value;

@Value
public class MyGroupEvent {

    private String type;
    private Map<String, Object> data;
    private long timestamp;

}
