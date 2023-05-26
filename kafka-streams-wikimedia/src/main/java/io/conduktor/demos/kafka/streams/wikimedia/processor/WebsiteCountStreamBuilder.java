package io.conduktor.demos.kafka.streams.wikimedia.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.conduktor.demos.kafka.streams.wikimedia.WikimediaStreamsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;

public class WebsiteCountStreamBuilder {
    private static final String WEBSITE_COUNT_STORE = "website-count-store";
    private static final String WEBSITE_COUNT_TOPIC = "wikimedia.stats.website";
    private static final String SERVER_NAME_KEY = "server_name";
    private static final String WEBSITE_KEY = "website";
    private static final String COUNT_KEY = "count";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TimeWindows TIME_WINDOWS = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));
    private static final Produced<Windowed<String>, String> PRODUCE_OPTIONS = Produced.with(
            WindowedSerdes.timeWindowedSerdeFrom(String.class, TIME_WINDOWS.size()),
            Serdes.String()
    );

    private final KStream<String, String> inputStream;

    public WebsiteCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        this.inputStream
                .selectKey((key, value) -> extractServerName(value))
                .groupByKey()
                .windowedBy(TIME_WINDOWS)
                .count(Materialized.as(WEBSITE_COUNT_STORE))
                .toStream()
                .mapValues(this::toJsonString)
                .to(WEBSITE_COUNT_TOPIC, PRODUCE_OPTIONS);
    }

    private String extractServerName(String value) {
        try {
            var jsonNode = OBJECT_MAPPER.readTree(value);
            return jsonNode.get(SERVER_NAME_KEY).asText();
        } catch (JsonProcessingException ex) {
            throw new WikimediaStreamsException("Could not parse JSON string", ex);
        }
    }

    private String toJsonString(Windowed<String> key, Long value) {
        var kvMap = Map.of(
                WEBSITE_KEY, key.key(),
                COUNT_KEY, value
        );
        try {
            return OBJECT_MAPPER.writeValueAsString(kvMap);
        } catch (JsonProcessingException ex) {
            throw new WikimediaStreamsException("Could not convert map to JSON string", ex);
        }
    }
}
