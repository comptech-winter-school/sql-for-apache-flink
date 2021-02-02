package ru.comptech2021.fliddhi;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

public class FliddhiExecutionOperator extends  AbstractStreamOperator<Row> implements OneInputStreamOperator<Row, Row> {

    private SiddhiAppRuntime siddhiAppRuntime; // вынести в executionEnvironment
    private InputHandler inputHandler;

    public FliddhiExecutionOperator(SiddhiManager siddhiManager, String sql) {
        this.siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sql);
        this.inputHandler = this.siddhiAppRuntime.getInputHandler("InputStream");
        siddhiAppRuntime.start(); //вынести в execution environment, try {} finally { siddhiApp.shutdown(); }

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                collectElements(events);
            }
        });
    }

    @Override public void processElement(StreamRecord<Row> streamRecord) throws Exception {
        inputHandler.send(record2Event(streamRecord));
    }


    private void collectElements(Event[] events){
        for(Event event : events) {
            StreamRecord<Row> record = event2Record(event);
            this.output.collect(record);
        }
    }

    private Event record2Event(StreamRecord<Row> record) { //todo перенести в StreamSchema
        Row row = record.getValue();
        int rowSize = row.getArity();
        Object[] fields = new Object[rowSize];
        for (int i=0; i < rowSize; i++) {
            fields[i] = row.getField(i); //todo надо еще вытащить названия и типы полей чтобы они соответствовали друг другу
        }
        return new Event(record.getTimestamp(), fields);
    }

    private StreamRecord<Row> event2Record(Event event) {
        // rows = siddhiApp.streamDefinitionMap.attributeList
        return null;
    }
}