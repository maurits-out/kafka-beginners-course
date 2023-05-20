package io.conduktor.demos.kafka.streams.wikimedia.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.conduktor.demos.kafka.streams.wikimedia.WikimediaStreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Map;

public final class BotCountStreamBuilder {

    private static final String BOT_FIELD_NAME = "bot";
    private static final String BOT_COUNT_STORE = "bot-count-store";
    private static final String BOT_COUNT_TOPIC = "wikimedia.stats.bots";
    private static final String BOT_TYPE = "bot";
    private static final String NON_BOT_TYPE = "non-bot";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KStream<String, String> inputStream;

    public BotCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        this.inputStream
                .mapValues(this::extractAuthorType)
                .groupBy((key, botOrNot) -> botOrNot)
                .count(Materialized.as(BOT_COUNT_STORE))
                .toStream()
                .mapValues(this::toJsonString)
                .to(BOT_COUNT_TOPIC);
    }

    private String toJsonString(String key, Long value) {
        var kvMap = Map.of(key, value);
        try {
            return OBJECT_MAPPER.writeValueAsString(kvMap);
        } catch (JsonProcessingException ex) {
            throw new WikimediaStreamsException("Could not convert map to JSON string", ex);
        }
    }

    private String extractAuthorType(String changeJson) {
        try {
            var jsonNode = OBJECT_MAPPER.readTree(changeJson);
            if (jsonNode.get(BOT_FIELD_NAME).asBoolean()) {
                return BOT_TYPE;
            }
            return NON_BOT_TYPE;
        } catch (JsonProcessingException ex) {
            throw new WikimediaStreamsException("Could not parse JSON", ex);
        }
    }
}
