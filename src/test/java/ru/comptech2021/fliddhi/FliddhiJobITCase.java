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
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(query);

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


        Row row1 = new Row(3);
        Row row2 = new Row(3);
        Row row3 = new Row(3);
        Row row4 = new Row(3);
        Row row5 = new Row(3);
        Row row6 = new Row(3);
        Row row7 = new Row(3);

        row1.setField(0, "IBM");
        row1.setField(1, 700f);
        row1.setField(2, 100L);

        row2.setField(0, "IBM");
        row2.setField(1, 700f);
        row2.setField(2, 100L);

        row3.setField(0, "WCO2");
        row3.setField(1, 60.5f);
        row3.setField(2, 20L);

        row4.setField(0, "GOOD");
        row4.setField(1, 50f);
        row4.setField(2, 30L);

        row5.setField(0, "IBM");
        row5.setField(1, 76.6f);
        row5.setField(2, 400L);

        row6.setField(0, "WCO2");
        row6.setField(1, 45.6f);
        row6.setField(2, 50L);

        row7.setField(0, "IBM");
        row7.setField(1, 50f);
        row7.setField(2, 30L);

        final DataStream<Row> sourceStream1 = env.fromElements(row1, row2, row3, row4, row5, row6, row7);

        String query = "" +
                "define stream SourceStream (symbol string, price float, volume long); " +
                "define stream SourceStream1 (symbol string, price float, volume long); " +
                "" +
                "from SourceStream select * insert into SourceStream1; " +
                "from SourceStream1[volume < 150] " +
                "select symbol, sum(price) as sumPrice group by symbol " +
                "insert into OutputStream;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("SourceStream", sourceStream1);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(query);

        // стандартный код флинка
        final DataStreamSink<Row> actual = outputStream
                .get("OutputStream") //debug here
                .print();

        env.execute();
    }
}
