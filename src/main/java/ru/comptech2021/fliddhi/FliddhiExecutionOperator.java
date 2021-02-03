package ru.comptech2021.fliddhi;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.*;

public class FliddhiExecutionOperator extends AbstractStreamOperator<FliddhiStream> implements OneInputStreamOperator<FliddhiStream, FliddhiStream> {

    private SiddhiManager siddhiManager;
    private SiddhiAppRuntime siddhiAppRuntime;
    private transient HashMap<String, InputHandler> siddhiInputHandlers = new HashMap<>(); // HashMap<SiddhiInputStreamName, InputHandler>

    public FliddhiExecutionOperator(SiddhiApp siddhiApp, Collection<String> inputStreamsName, Collection<String> outputStreamsName) {
        this.siddhiManager = new SiddhiManager();
        this.siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        inputStreamsName.forEach(name -> siddhiInputHandlers.put(name, siddhiAppRuntime.getInputHandler(name)));
        outputStreamsName.forEach(name ->
                siddhiAppRuntime.addCallback(name, new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        collectElements(events);
                    }
                }));
    }

    @Override
    public void processElement(StreamRecord<FliddhiStream> streamRecord) throws Exception {
        String flinkStreamName = streamRecord.getValue().name();
        siddhiInputHandlers.get(flinkStreamName).send(record2Event(streamRecord));
    }


    private void collectElements(Event[] events) {
        for (Event event : events) {
            StreamRecord<FliddhiStream> record = event2Record(event);
            this.output.collect(record);
        }
    }

    private Event record2Event(StreamRecord<FliddhiStream> record) { //todo вынести в StreamSchema/Converter, отрефакторить метод
        FliddhiStream stream = record.getValue();
        String streamName = stream.name();

        Map<String, StreamDefinition> siddhiStreamDefinition = siddhiAppRuntime.getStreamDefinitionMap();
        //в случае если StreamDefinition пуст, объявляем его и наполняем аттрибутами
        StreamDefinition streamDefinition = siddhiStreamDefinition.getOrDefault(streamName, new StreamDefinition());
        streamDefinition.setId(streamName); //потенциально лишняя операция
        List<Attribute> attributeList = streamDefinition.getAttributeList();
        if (attributeList.isEmpty()){
            attributeList.addAll(stream.dataStream().map(row -> new Attribute(???)));
        }
        attributeList.forEach(attr -> streamDefinition.attribute(attr.getName(), attr.getType()));

        // convert fliddhi stream to event
    }

    private StreamRecord<FliddhiStream> event2Record(Event event) {
        return null;
    }

    @Override
    public void open() throws Exception {
        super.open();
        siddhiAppRuntime.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        siddhiManager.shutdown();
        siddhiAppRuntime.shutdown();
    }
}