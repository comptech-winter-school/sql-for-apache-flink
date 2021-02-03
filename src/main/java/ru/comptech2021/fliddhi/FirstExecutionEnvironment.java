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

import java.util.*;


class FirstExecutionEnvironment implements FliddhiExecutionEnvironment {

    private final StreamExecutionEnvironment env;
    private HashMap<String, DataStream<Tuple2<String, Row>>> registeredInputStreams = new HashMap<>();
    private ArrayList<String> namesOfOutputStreams = new ArrayList<>();
    private ArrayList<String> namesOfInputStreams = new ArrayList<>();
    private DataStream<Tuple2<String, Row>> unionStream;

    public ArrayList<String> getNamesOfInputStreams() {
        return namesOfInputStreams;
    }

    public FirstExecutionEnvironment(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public HashMap<String, DataStream<Tuple2<String, Row>>> getRegisteredInputStreams() {
        return registeredInputStreams;
    }

    @Override
    public Map<String, DataStream<Row>> siddhiQL(String query) {
        new FliddhiExecutionOperator(query);

        SiddhiApp siddhiApp = SiddhiCompiler.parse(SiddhiCompiler.updateVariables(query));
        namesOfOutputStreams.add(((Query)siddhiApp.getExecutionElementList().get(0)).getOutputStream().getId());

        HashMap<String, DataStream<Row>> outputStream = new HashMap<>();
//        outputStream.put(nameOfOutputStream,);
        return outputStream;//ключ это название аутпут стрима в стринге, а значение это оператор
    }

    public ArrayList<String> getNamesOfOutputStreams() {
        return namesOfOutputStreams;
    }

    public void registerInputStream(String nameOfStream, DataStream<Row> nameOfDataStream) {
        registeredInputStreams.put(nameOfStream, nameOfDataStream.map(row -> Tuple2.of(nameOfStream,row)));
        namesOfInputStreams.add(nameOfStream);
    }

    public DataStream<Tuple2<String, Row>> getUnionStream () {
        List<DataStream<Tuple2<String, Row>>> streams = (List<DataStream<Tuple2<String, Row>>>) registeredInputStreams.values();
        unionStream = streams.get(0);
        for(int i = 1; i<streams.size(); i++){
            unionStream = unionStream.union(streams.get(i));
        }
        return unionStream;
    }

    public Map<String,DataStream<Row>> outputRecordRouting(
            DataStream<Tuple2<String, Row>> outputStreams){

        Map<String,DataStream<Row>> outputMap = new HashMap<>();
        for (int i = 0; i < namesOfOutputStreams.size(); i++) {
            int finalI = i;
            outputMap.put(namesOfOutputStreams.get(i),outputStreams.filter(
                    (FilterFunction<Tuple2<String, Row>>) stringRowTuple2 ->
                            stringRowTuple2.f0.equals(namesOfOutputStreams.get(finalI))).
                    map((MapFunction<Tuple2<String, Row>, Row>) stringRowTuple2 -> stringRowTuple2.f1));
        }

        return outputMap;
    }
}
