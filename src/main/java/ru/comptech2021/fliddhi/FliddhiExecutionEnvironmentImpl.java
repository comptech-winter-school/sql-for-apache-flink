package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.compiler.SiddhiCompiler;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class FliddhiExecutionEnvironmentImpl implements FliddhiExecutionEnvironment {

    private final StreamExecutionEnvironment env;

    private final ArrayList<String> namesOfInputStreams = new ArrayList<>();
    private final ArrayList<String> namesOfOutputStreams = new ArrayList<>();
    private final HashMap<String, DataStream<Tuple2<String, Row>>> registeredInputStreams = new HashMap<>();

    public FliddhiExecutionEnvironmentImpl(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void registerInputStream(String nameOfStream, DataStream<Row> dataStream) {
        registeredInputStreams.put(nameOfStream, dataStream.map(row -> Tuple2.of(nameOfStream, row)));
        namesOfInputStreams.add(nameOfStream);
    }

    @Override
    public Map<String, DataStream<Row>> siddhiQL(int parallelism, String query) {
        //todo в зависимости от parallelism выбирать keySelector

        SiddhiApp siddhiApp = new SiddhiApp(query);
        new FliddhiExecutionOperator(siddhiApp, namesOfInputStreams, namesOfOutputStreams);

        //SiddhiApp siddhiApp = SiddhiCompiler.parse(SiddhiCompiler.updateVariables(query));
        namesOfOutputStreams.add(((Query) siddhiApp.getExecutionElementList().get(0)).getOutputStream().getId());

        HashMap<String, DataStream<Row>> outputStream = new HashMap<>();
//        outputStream.put(nameOfOutputStream,);
        return outputStream; //ключ это название аутпут стрима в стринге, а значение это оператор
    }

    public DataStream<Tuple2<String, Row>> getUnionStream() {
        List<DataStream<Tuple2<String, Row>>> streams = (List<DataStream<Tuple2<String, Row>>>) registeredInputStreams.values();
        DataStream<Tuple2<String, Row>> unionStream = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            unionStream = unionStream.union(streams.get(i));
        }
        return unionStream;
    }

    public Map<String, DataStream<Row>> outputRecordRouting(DataStream<Tuple2<String, Row>> outputStreams) {
        Map<String, DataStream<Row>> outputMap = new HashMap<>();
        for (int i = 0; i < namesOfOutputStreams.size(); i++) {
            int finalI = i;
            outputMap.put(namesOfOutputStreams.get(i), outputStreams.filter(
                    (FilterFunction<Tuple2<String, Row>>) stringRowTuple2 ->
                            stringRowTuple2.f0.equals(namesOfOutputStreams.get(finalI))).
                    map((MapFunction<Tuple2<String, Row>, Row>) stringRowTuple2 -> stringRowTuple2.f1));
        }
        return outputMap;
    }

    //region Getters and setters
    public ArrayList<String> getNamesOfInputStreams() { return namesOfInputStreams; }

    public ArrayList<String> getNamesOfOutputStreams() {
        return namesOfOutputStreams;
    }

    public HashMap<String, DataStream<Tuple2<String, Row>>> getRegisteredInputStreams() { return registeredInputStreams; }

    //endregion

}
