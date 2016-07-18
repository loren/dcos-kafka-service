package com.mesosphere.dcos.kafka.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * HeapConfig contains the configuration for the JVM heap for a Kafka
 * broker.
 */
public class HeapConfig {
    @JsonProperty("sizeMb")
    private int sizeMb;

    public HeapConfig() {

    }

    @JsonCreator
    public HeapConfig(final int sizeMb) {
        this.sizeMb = sizeMb;
    }

    public int getSizeMb() {
        return sizeMb;
    }

    @JsonProperty("sizeMb")
    public void setSizeMb(int sizeMb) {
        this.sizeMb = sizeMb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HeapConfig that = (HeapConfig) o;
        return sizeMb == that.sizeMb;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sizeMb);
    }

    @Override
    public String toString() {
        return "HeapConfig{" +
                "sizeMb=" + sizeMb +
                '}';
    }
}
