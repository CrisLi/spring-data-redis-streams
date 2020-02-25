package org.chris.demo.redis.web;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.chris.demo.redis.streams.MyGroupStreamService;
import org.chris.demo.redis.streams.MyStreamService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class RedisWebController {

    private final MyStreamService streamService;

    private final MyGroupStreamService groupStreamService;

    @GetMapping("/messages/{message}")
    public void myStreamEvent(@PathVariable String message) {
        streamService.sendMessage(message);
    }

    @GetMapping("/events")
    public void myGroupStreamEvent(@RequestParam String name,
        @RequestParam String sex,
        @RequestParam int age) {

        Map<String, Object> data = new HashMap<>(3);

        data.put("name", name);
        data.put("sex", sex);
        data.put("age", age);

        groupStreamService.sendEvent(data);
    }

    @GetMapping("/events/huge")
    public void huge() {

        Map<String, Object> data = new HashMap<>(3);

        data.put("name", "Chris");
        data.put("sex", "Male");
        data.put("age", 35);

        IntStream.range(0, 100_000).parallel()
            .forEach(i -> {
                groupStreamService.sendEvent(data);
            });

    }
}
