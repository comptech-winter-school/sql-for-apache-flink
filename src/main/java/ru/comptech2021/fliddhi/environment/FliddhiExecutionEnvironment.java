package ru.comptech2021.fliddhi.environment;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * Represents environment that provides Siddhi API in Flink.
 */
public interface FliddhiExecutionEnvironment {

    /**
     * Creates an execution environment that represents Siddhi API in Flink.
     *
     * @param env flink execution environment
     * @throws NullPointerException if env is null
     * @return implementation of {@link FliddhiExecutionEnvironment}
     */
    static FliddhiExecutionEnvironment getExecutionEnvironment(StreamExecutionEnvironment env){
        return new FliddhiExecutionEnvironmentImpl(env);
    };

    /**
     * Executes siddhi sql.
     *
     * @param query query executed by Siddhi library
     * @throws NullPointerException if any parameters are null
     * @return output stream
     */
    Map<String, DataStream<Row>> siddhiQL(String query);

    void registerInputStream(String nameOfStream, DataStream<Row> dataStream);
}
