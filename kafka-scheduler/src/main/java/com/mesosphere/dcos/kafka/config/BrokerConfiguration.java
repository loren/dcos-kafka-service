package com.mesosphere.dcos.kafka.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class BrokerConfiguration {
    @JsonProperty("cpus")
    private double cpus;
    @JsonProperty("mem")
    private double mem;
    @JsonProperty("heap")
    private HeapConfig heap;
    @JsonProperty("disk")
    private double disk;
    @JsonProperty("diskType")
    private String diskType;
    @JsonProperty("kafkaUri")
    private String kafkaUri;
    @JsonProperty("javaUri")
    private String javaUri;
    @JsonProperty("overriderUri")
    private String overriderUri;
    @JsonProperty("port")
    private Long port;

    public BrokerConfiguration() {

    }

    @JsonCreator
    public BrokerConfiguration(
            @JsonProperty("cpus")double cpus,
            @JsonProperty("mem")double mem,
            @JsonProperty("heap")HeapConfig heap,
            @JsonProperty("disk")double disk,
            @JsonProperty("diskType")String diskType,
            @JsonProperty("kafkaUri")String kafkaUri,
            @JsonProperty("javaUri")String javaUri,
            @JsonProperty("overriderUri")String overriderUri,
            @JsonProperty("port")Long port) {
        this.cpus = cpus;
        this.mem = mem;
        this.heap = heap;
        this.disk = disk;
        this.diskType = diskType;
        this.kafkaUri = kafkaUri;
        this.javaUri = javaUri;
        this.overriderUri = overriderUri;
        this.port = port;
    }

    public double getCpus() {
        return cpus;
    }

    @JsonProperty("cpus")
    public void setCpus(double cpus) {
        this.cpus = cpus;
    }

    public double getMem() {
        return mem;
    }

    @JsonProperty("mem")
    public void setMem(double mem) {
        this.mem = mem;
    }

    public HeapConfig getHeap() {
        return heap;
    }

    @JsonProperty("heap")
    public void setHeap(HeapConfig heap) {
        this.heap = heap;
    }

    public double getDisk() {
        return disk;
    }

    @JsonProperty("disk")
    public void setDisk(double disk) {
        this.disk = disk;
    }

    public String getDiskType() {
        return diskType;
    }

    @JsonProperty("diskType")
    public void setDiskType(String diskType) {
        this.diskType = diskType;
    }

    public String getKafkaUri() {
        return kafkaUri;
    }

    @JsonProperty("kafkaUri")
    public void setKafkaUri(String kafkaUri) {
        this.kafkaUri = kafkaUri;
    }

    public String getJavaUri() {
        return javaUri;
    }

    @JsonProperty("javaUri")
    public void setJavaUri(String javaUri) {
        this.javaUri = javaUri;
    }

    public String getOverriderUri() {
        return overriderUri;
    }

    @JsonProperty("overriderUri")
    public void setOverriderUri(String overriderUri) {
        this.overriderUri = overriderUri;
    }

    public Long getPort() {
        return port;
    }

    @JsonProperty("port")
    public void setPort(Long port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
          return true;
        }

        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        BrokerConfiguration that = (BrokerConfiguration) o;
        return Double.compare(that.cpus, cpus) == 0 &&
                Double.compare(that.mem, mem) == 0 &&
                Objects.equals(that.heap, heap) &&
                Double.compare(that.disk, disk) == 0 &&
                Objects.equals(diskType, that.diskType) &&
                Objects.equals(kafkaUri, that.kafkaUri) &&
                Objects.equals(javaUri, that.javaUri) &&
                Objects.equals(overriderUri, that.overriderUri) &&
                Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpus, mem, heap, disk, diskType, kafkaUri, javaUri, overriderUri, port);
    }

    @Override
    public String toString() {
        return "BrokerConfiguration{" +
                "cpus=" + cpus +
                ", mem=" + mem +
                ", heap=" + heap +
                ", disk=" + disk +
                ", diskType='" + diskType + '\'' +
                ", kafkaUri='" + kafkaUri + '\'' +
                ", javaUri='" + javaUri + '\'' +
                ", overriderUri='" + overriderUri + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
