package io.conduktor.demos.kafka.streams.wikimedia.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.conduktor.demos.kafka.streams.wikimedia.WikimediaStreamsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;

public final class EventCountTimeSeriesBuilder {

    private static final String TIME_SERIES_TOPIC = "wikimedia.stats.timeseries";
    private static final String TIME_SERIES_STORE = "event-count-store";
    private static final String KEY_TO_GROUP = "key-to-group";
    private static final String START_TIME_KEY = "start_time";
    private static final String END_TIME_KEY = "end_time";
    private static final String WINDOW_SIZE_KEY = "window_size";
    private static final String EVENT_COUNT_KEY = "event_count";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TimeWindows TIME_WINDOWS = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10));
    private static final Produced<Windowed<String>, String> PRODUCE_OPTIONS = Produced.with(
            WindowedSerdes.timeWindowedSerdeFrom(String.class, TIME_WINDOWS.size()),
            Serdes.String()
    );

    private final KStream<String, String> inputStream;

    public EventCountTimeSeriesBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        this.inputStream
                .selectKey((key, value) -> KEY_TO_GROUP)
                .groupByKey()
                .windowedBy(TIME_WINDOWS)
                .count(Materialized.as(TIME_SERIES_STORE))
                .toStream()
                .mapValues(this::toJsonString)
                .to(TIME_SERIES_TOPIC, PRODUCE_OPTIONS);
    }

    private String toJsonString(Windowed<String> readOnlyKey, Long value) {
        var kvMap = Map.of(
                START_TIME_KEY, readOnlyKey.window().startTime().toString(),
                END_TIME_KEY, readOnlyKey.window().endTime().toString(),
                WINDOW_SIZE_KEY, TIME_WINDOWS.size(),
                EVENT_COUNT_KEY, value
        );
        try {
            return OBJECT_MAPPER.writeValueAsString(kvMap);
        } catch (JsonProcessingException ex) {
            throw new WikimediaStreamsException("Could not convert map to JSON string", ex);
        }
    }
}
