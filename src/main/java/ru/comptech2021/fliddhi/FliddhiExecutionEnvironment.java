package ru.comptech2021.fliddhi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
    static FliddhiExecutionEnvironment getExecutionEnvironment(StreamExecutionEnvironment env) {
        throw new UnsupportedOperationException("FliddhiExecutionEnvironment.getExecutionEnvironment");
    }

    /**
     * Executes siddhi sql.
     *
     * @param query query executed by Siddhi library
     * @param streams streams used in the query
     * @throws NullPointerException if any parameters are null
     * @return output stream
     */
    FliddhiStream siddhiQL(String query, FliddhiStream... streams);
}
