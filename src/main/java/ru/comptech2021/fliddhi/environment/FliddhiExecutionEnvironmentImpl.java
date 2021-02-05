package ru.comptech2021.fliddhi.environment;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.compiler.SiddhiCompiler;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import ru.comptech2021.fliddhi.FliddhiExecutionOperator;
import ru.comptech2021.fliddhi.FliddhiPlanner;
import ru.comptech2021.fliddhi.FlinkRecord;
import ru.comptech2021.fliddhi.selector.FliddhiKeySelector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class FliddhiExecutionEnvironmentImpl implements FliddhiExecutionEnvironment {

    private final StreamExecutionEnvironment env;

    private final ArrayList<String> namesOfInputStreams = new ArrayList<>();
    private final ArrayList<String> namesOfOutputStreams = new ArrayList<>();
    private  HashMap<String, DataStream<FlinkRecord>> registeredInputStreams = new HashMap<>();

    public FliddhiExecutionEnvironmentImpl(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void registerInputStream(String nameOfStream, DataStream<Row> dataStream) {
        registeredInputStreams.put(nameOfStream, dataStream.map(row -> new FlinkRecord(nameOfStream, row)));
        namesOfInputStreams.add(nameOfStream);
    }

    @Override
    public Map<String, DataStream<Row>> siddhiQL(String query) {
        int parallelism = env.getParallelism();
        SiddhiApp siddhiApp = SiddhiCompiler.parse(SiddhiCompiler.updateVariables(query));

        namesOfOutputStreams.add(((Query) siddhiApp.getExecutionElementList().get(0)).getOutputStream().getId());

        FliddhiKeySelector keySelector = FliddhiPlanner.createFliddhiKeySelector(siddhiApp);
        FliddhiExecutionOperator operator = new FliddhiExecutionOperator(siddhiApp, namesOfInputStreams, namesOfOutputStreams);

        DataStream<FlinkRecord> streams = unionStreams();

        if (parallelism != 1){
            streams = streams.keyBy(keySelector);
        }
        DataStream<FlinkRecord> resultStream = streams.transform("Siddhi Query", TypeInformation.of(FlinkRecord.class), operator);

        return outputRecordRouting(resultStream);
    }

    private DataStream<FlinkRecord> unionStreams() {
        List<DataStream<FlinkRecord>> streams = new ArrayList<>(registeredInputStreams.values());
        DataStream<FlinkRecord> unionStream = streams.get(0);
        for (int i = 1; i < streams.size(); i++)
            unionStream = unionStream.union(streams.get(i));
        return unionStream;
    }

    private Map<String, DataStream<Row>> outputRecordRouting(DataStream<FlinkRecord> outputStreams) {
        Map<String, DataStream<Row>> outputMap = new HashMap<>();
        for (int i = 0; i < namesOfOutputStreams.size(); i++) {
            int finalI = i;
            ArrayList<String> outputNames = this.namesOfOutputStreams;
            outputMap.put(
                    outputNames.get(i),
                    outputStreams
                            .filter(record -> record.getStreamName().equals(outputNames.get(finalI)))
                            .map(FlinkRecord::getRow));
        }
        return outputMap;
    }

    //region Getters and setters

    public ArrayList<String> getNamesOfInputStreams() {
        return namesOfInputStreams;
    }

    public ArrayList<String> getNamesOfOutputStreams() { return namesOfOutputStreams; }

    public HashMap<String, DataStream<FlinkRecord>> getRegisteredInputStreams() {
        return registeredInputStreams;
    }

    //endregion

}
