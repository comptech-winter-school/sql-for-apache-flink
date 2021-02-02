package ru.comptech2021.fliddhi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;


class FirstExecutionEnvironment implements FliddhiExecutionEnvironment {
    private final StreamExecutionEnvironment env;
    private HashMap<String, DataStream<Row>> registeredInputStreams = new HashMap<>();
    private String nameOfOutputStream;

    public FirstExecutionEnvironment(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public HashMap<String, DataStream<Row>> getRegisteredInputStreams() {
        return registeredInputStreams;
    }

    @Override
    public Map<String, DataStream<Row>> siddhiQL(String query) {
        new FliddhiExecutionOperator(query);
        String[] words = query.split("\\s");
        for (int i = 0; i != words.length; i++) {
            if (words[i].equals("INTO")) {
                nameOfOutputStream = words[i + 1];
            }
        }
        HashMap<String, DataStream<Row>> outputStream = new HashMap<>();
//        outputStream.put(nameOfOutputStream,);
        return outputStream;//ключ это название аутпут стрима в стринге, а значение это оператор
    }

    public void registerInputStream(String nameOfStream, DataStream<Row> nameOfDataStream) {
        registeredInputStreams.put(nameOfStream, nameOfDataStream);
    }
}
