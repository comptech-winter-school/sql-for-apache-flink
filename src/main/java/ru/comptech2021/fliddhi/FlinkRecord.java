package ru.comptech2021.fliddhi;

import org.apache.flink.types.Row;

import java.io.Serializable;

public class FlinkRecord implements Serializable {
    // todo timestamp?

    private String streamName;
    private Row row;

    public FlinkRecord(String streamName, Row row) {
        this.streamName = streamName;
        this.row = row;
    }

    public String getStreamName() {
        return streamName;
    }

    public Row getRow() {
        return row;
    }
}
