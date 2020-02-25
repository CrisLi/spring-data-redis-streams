package org.chris.demo.redis.streams;

import java.time.Duration;

import javax.annotation.PostConstruct;

import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.data.redis.stream.StreamReceiver.StreamReceiverOptions;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class MyReactiveStreamReceiver {

    private final ReactiveRedisConnectionFactory connectionFactory;

    private final ReactiveStringRedisTemplate redisTemplate;

    @Getter
    private final String groupName = "first-group";

    private Consumer consumer = Consumer.from(groupName, "reactive-consumer-1");

    @PostConstruct
    public void init() {

        ackPEL();

        StreamReceiverOptions<String, ObjectRecord<String, MyGroupEvent>> options = StreamReceiverOptions.builder()
            .pollTimeout(Duration.ofSeconds(1))
            .batchSize(200)
            .targetType(MyGroupEvent.class)
            .build();

        StreamReceiver<String, ObjectRecord<String, MyGroupEvent>> receiver = StreamReceiver.create(connectionFactory, options);

        StreamOffset<String> offset = StreamOffset.create(MyGroupStreamService.STREAM_NAME, ReadOffset.lastConsumed());

        receiver.receive(consumer, offset)
            .flatMap(this::onMessage)
            .subscribe();

    }

    private Mono<Long> onMessage(ObjectRecord<String, MyGroupEvent> message) {

        log.info("StreamReceiver received Stream[{}], event[{}]", message.getStream(), message.getValue());

        try {
            // process
            return redisTemplate.opsForStream().acknowledge(groupName, message);
        } catch (Exception e) {
            // TODO The exception must be handled by yourself
            return Mono.just(-1L);
        }

    }

    @SuppressWarnings("unchecked")
    private void ackPEL() {

        StreamOffset<String> pmlOffset = StreamOffset.fromStart(MyGroupStreamService.STREAM_NAME);

        redisTemplate.opsForStream().read(MyGroupEvent.class, consumer, pmlOffset)
            .flatMap(r -> {
                log.info("Ack record[{}] in the pending list", r.getId());
                return redisTemplate.opsForStream().acknowledge(groupName, r);
            })
            .subscribe();
    }

}
