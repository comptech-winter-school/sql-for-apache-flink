package ru.comptech2021.fliddhi;


import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;
import ru.comptech2021.fliddhi.environment.FliddhiExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FliddhiJobITCase {


    @Test
    public void jobShouldTransferIntegersFromSourceToOutStream() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(1, 2, 3, 4, 5).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String query = "" +
                "define stream SourceStream (number int); " +
                "define stream OutputStream (number int); " +
                "" +
                "FROM SourceStream SELECT number INSERT INTO OutputStream";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("SourceStream", sourceStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(1, query);

        // стандартный код флинка
        final DataStreamSink<Integer> actual = outputStream
                .get("OutputStream")
                .map(row -> (Integer) row.getField(0))
                .print();
        env.execute();

//                .executeAndCollect(5);
//        assertThat(actual, containsInAnyOrder(1, 2, 3, 4, 5));
    }

    @Test
    public void test2() throws Exception {
        Object[] elements = {
                new Object[]{"IBM", 700f, 100L},
                new Object[]{"IBM", 700f, 100L},
                new Object[]{"WSO2", 60.5f, 20},
                new Object[]{"GOOG", 50f, 30L},
                new Object[]{"IBM", 76.6f, 400L},
                new Object[]{"WSO2", 45.6f, 50L},
                new Object[]{"GOOG", 50f, 30L}};

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(elements).map(Row::of);

        String query = "" +
                "define stream SourceStream (symbol string, price float, volume long); " +
                "" +
                "from SourceStream[volume < 150] " +
                "select symbol, sum(price) as sumPrice group by symbol " +
                "insert into OutputStream;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("SourceStream", sourceStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(1, query);

        // стандартный код флинка
        final DataStreamSink<Row> actual = outputStream
                .get("OutputStream") //debug here
                .print();

        env.execute();
    }
}
