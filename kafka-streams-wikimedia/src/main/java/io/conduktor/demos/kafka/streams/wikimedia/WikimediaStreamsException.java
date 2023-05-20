package io.conduktor.demos.kafka.streams.wikimedia;

public class WikimediaStreamsException extends RuntimeException {

    public WikimediaStreamsException(String message, Throwable cause) {
        super(message, cause);
    }
}
