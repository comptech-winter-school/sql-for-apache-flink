package ru.comptech2021.fliddhi;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

public class FliddhiExecutionOperator extends AbstractStreamOperator<Row> implements OneInputStreamOperator<Row,Row> {

    public FliddhiExecutionOperator(String query){

    }

    @Override
    public void processElement(StreamRecord<Row> streamRecord) throws Exception {

    }
}
