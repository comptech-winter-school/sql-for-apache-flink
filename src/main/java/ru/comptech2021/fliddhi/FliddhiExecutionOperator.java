package ru.comptech2021.fliddhi;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.query.api.SiddhiApp;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import scala.Tuple2;

import java.util.*;

public class FliddhiExecutionOperator extends AbstractStreamOperator<Tuple2<String, Row>> implements OneInputStreamOperator<Tuple2<String, Row>, Tuple2<String, Row>> {

    private transient SiddhiManager siddhiManager; //возможно что-то здесь тосит удалить
    private transient SiddhiApp siddhiApp;
    private transient SiddhiAppRuntime siddhiAppRuntime;

    private Collection<String> inputStreamsName;
    private Collection<String> outputStreamsName;
    private transient HashMap<String, InputHandler> siddhiInputHandlers = new HashMap<>(); // HashMap<SiddhiInputStreamName, InputHandler>

    public FliddhiExecutionOperator(SiddhiApp siddhiApp, Collection<String> inputStreamsName, Collection<String> outputStreamsName) {
        this.siddhiApp = siddhiApp;
        this.inputStreamsName= inputStreamsName;
        this.outputStreamsName = outputStreamsName;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.siddhiManager = new SiddhiManager();
        this.siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(this.siddhiApp);

        inputStreamsName.forEach(name -> siddhiInputHandlers.put(name, siddhiAppRuntime.getInputHandler(name)));
        outputStreamsName.forEach(name ->
                siddhiAppRuntime.addCallback(name, new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        collectElements(name, events);
                    }
                }));

        siddhiAppRuntime.start();
    }

    @Override
    public void processElement(StreamRecord<Tuple2<String, Row>> streamRecord) throws Exception {
        String flinkStreamName = streamRecord.getValue()._1;
        Row row = streamRecord.getValue()._2;
        siddhiInputHandlers.get(flinkStreamName).send(row2Event(row));
    }

    private void collectElements(String streamName, Event[] events) {
        for (Event event : events) {
            StreamRecord<Tuple2<String, Row>> record = new StreamRecord<>(new Tuple2<>(streamName, event2Row(event)));
            this.output.collect(record);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        siddhiManager.shutdown();
        siddhiAppRuntime.shutdown();
    }

    private Event row2Event(Row row) { //todo вынести в StreamSchema/Converter, отрефакторить метод
        int rowSize = row.getArity();
        Object[] fields = new Object[rowSize];
        for (int i = 0; i < rowSize; i++) {
            fields[i] = row.getField(i);
        }
        Event event = new Event();
        event.setData(fields);
        return event;
    }

    private Row event2Row(Event event) {
        Object[] data = event.getData();
        int dataLength = data.length;
        Row row = new Row(dataLength); // todo set RowKind?
        for (int i = 0; i < dataLength; i++) {
            row.setField(i, data[i]);
        }
        return row;
    }

}