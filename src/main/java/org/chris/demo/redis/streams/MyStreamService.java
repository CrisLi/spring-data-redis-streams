package org.chris.demo.redis.streams;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

// See https://docs.spring.io/spring-data/data-redis/docs/current/reference/html/#redis.streams

/*
 * 100,000 messages will use 120M memory, 
 * 
 */
@Service
@RequiredArgsConstructor
public class MyStreamService implements ApplicationRunner {

    public static final String STREAM_NAME = "myStream";

    private final StringRedisTemplate redisTemplate;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        Map<String, String> startup = new HashMap<>();

        startup.put("type", "startup");
        startup.put("data", "none");
        startup.put("timestamp", String.valueOf(System.currentTimeMillis()));

        StringRecord record = StreamRecords.string(startup).withStreamKey(STREAM_NAME);

        redisTemplate.opsForStream().add(record);

    }

    public void sendMessage(String message) {

        Map<String, String> startup = new HashMap<>();

        startup.put("type", "send");
        startup.put("data", message);
        startup.put("timestamp", String.valueOf(System.currentTimeMillis()));

        StringRecord record = StreamRecords.string(startup).withStreamKey(STREAM_NAME);

        redisTemplate.opsForStream().add(record);
    }
}
