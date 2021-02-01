package ru.comptech2021.fliddhi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.stream.Stream;

/**
 * Represents stream using by Siddhi library.
 */
public interface FliddhiStream {

    /**
     * Creates a stream that represents siddhi stream.
     * Arity of a row should be equal to number of fields
     *
     * @param name name of stream
     * @param dataStream flink data stream of rows
     * @param fields names of a row fields
     * @throws NullPointerException if any parameters are null
     * @return implementation of {@link FliddhiStream}.
     */
    static FliddhiStream of(String name, DataStream<Row> dataStream, String... fields) {
        throw new UnsupportedOperationException("FliddhiStream.of");
    }

    /**
     * @return name of stream
     */
    String name();

    /**
     * @return flink data stream of rows
     */
    DataStream<Row> dataStream();

    /**
     * @return names of a row fields
     */
    Stream<String> fields();
}
