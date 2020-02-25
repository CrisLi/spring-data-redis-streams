package org.chris.demo.redis.streams;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;
import org.springframework.util.ErrorHandler;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class MyGroupStreamListener implements StreamListener<String, ObjectRecord<String, MyGroupEvent>> {

    private final StringRedisTemplate redisTemplate;

    @Getter
    private final ErrorHandler errorHandler = (error) -> {
        log.error("MyGroupStreamListener error", error);
    };

    @Getter
    private final String groupName = "first-group";

    private AtomicInteger counter = new AtomicInteger(0);
    private AtomicLong cost = new AtomicLong(0L);

    @Getter
    @Value("${myGroupStreamListener.name:first}")
    private String consumerName;

    @PostConstruct
    public void init() {

        Consumer consumer = Consumer.from(groupName, consumerName);

        StreamOffset<String> offset = StreamOffset.<String>fromStart(MyGroupStreamService.STREAM_NAME);

        @SuppressWarnings("unchecked")
        List<ObjectRecord<String, MyGroupEvent>> records = redisTemplate.opsForStream().<MyGroupEvent>read(
            MyGroupEvent.class,
            consumer,
            StreamReadOptions.empty(),
            offset);

        records.forEach(r -> {
            redisTemplate.opsForStream().acknowledge(this.getGroupName(), r);
            log.info("Ack record[{}] in the pending list", r.getId());
        });

    }

    @Override
    public void onMessage(ObjectRecord<String, MyGroupEvent> message) {

        // see StreamPollTask.doLoop(K key)
        // and
        // DefaultStreamMessageListenerContainer.getReadFunction

        long start = System.nanoTime();

        log.info("StreamListener received Stream[{}], event[{}]", message.getStream(), message.getValue());

        int i = counter.incrementAndGet();

        log.info("{} record(s) consumed", i);

        redisTemplate.opsForStream().acknowledge(this.getGroupName(), message);

        long costNano = System.nanoTime() - start;

        long total = cost.addAndGet(costNano);

        log.info("cost {} ms", Duration.ofNanos(total).toMillis());
    }

}
