package org.chris.demo.redis.config;

import java.time.Duration;

import org.chris.demo.redis.streams.MyGroupEvent;
import org.chris.demo.redis.streams.MyGroupStreamListener;
import org.chris.demo.redis.streams.MyGroupStreamService;
import org.chris.demo.redis.streams.MyStreamListener;
import org.chris.demo.redis.streams.MyStreamService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamReadRequest;

@Configuration
public class RedisConfig {

    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> myStreamContainer(
        RedisConnectionFactory connectionFactory,
        MyStreamListener streamListener) {

        StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options = StreamMessageListenerContainerOptions
            .builder()
            .pollTimeout(Duration.ofSeconds(1))
            .batchSize(100)
            .build();

        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
            .create(connectionFactory, options);

        // for the no-group consumer, you have to control the last offset yourself
        // see
        // https://docs.spring.io/spring-data/data-redis/docs/current/reference/html/#redis.streams.receive.readoffset
        container.receive(StreamOffset.latest(MyStreamService.STREAM_NAME), streamListener);

        container.start();

        return container;
    }

    @Bean
    public StreamMessageListenerContainer<String, ObjectRecord<String, MyGroupEvent>> myGroupStreamContainer(
        RedisConnectionFactory connectionFactory,
        MyGroupStreamListener streamListener) {

        StreamMessageListenerContainerOptions<String, ObjectRecord<String, MyGroupEvent>> options = StreamMessageListenerContainerOptions
            .builder()
            .pollTimeout(Duration.ofSeconds(1))
            .batchSize(100)
            .targetType(MyGroupEvent.class)
            .executor(messageExecutor())
            .build();

        StreamMessageListenerContainer<String, ObjectRecord<String, MyGroupEvent>> container = StreamMessageListenerContainer
            .create(connectionFactory, options);

        StreamOffset<String> offset = StreamOffset.create(MyGroupStreamService.STREAM_NAME, ReadOffset.lastConsumed());

        Consumer consumer = Consumer.from(streamListener.getGroupName(), streamListener.getConsumerName());

        StreamReadRequest<String> streamReadRequest = StreamReadRequest.builder(offset)
            .errorHandler(streamListener.getErrorHandler())
            .cancelOnError(e -> false)
            .consumer(consumer)
            .autoAcknowledge(false)
            .build();

        // AutoAck bug: https://github.com/spring-projects/spring-data-redis/pull/508

        container.register(streamReadRequest, streamListener);

        container.start();

        return container;
    }

    public SimpleAsyncTaskExecutor messageExecutor() {

        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();

        executor.setThreadNamePrefix("myStreamTask-");
        executor.setConcurrencyLimit(10);

        return executor;
    }
}
