package org.chris.demo.redis.streams;

import java.util.Map;

import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class MyGroupStreamService {

    public static final String STREAM_NAME = "myGroupStream";

    private final StringRedisTemplate redisTemplate;

    public void sendEvent(Map<String, Object> data) {

        MyGroupEvent event = new MyGroupEvent("my-group-event", data, System.currentTimeMillis());

        ObjectRecord<String, MyGroupEvent> record = StreamRecords.objectBacked(event).withStreamKey(STREAM_NAME);

        redisTemplate.opsForStream().add(record);

    }
}
