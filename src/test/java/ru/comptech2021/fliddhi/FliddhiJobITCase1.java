package ru.comptech2021.fliddhi;


import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.compiler.SiddhiCompiler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FliddhiJobITCase1 {

    @Test
    public void jobShouldTransferIntegersFromSourceToOutStream() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(1, 2, 3, 4, 5).map(Row::of);

        Row row1 = new Row(3);
        Row row2 = new Row(3);
        Row row3 = new Row(3);

        row1.setField(0,10);
        row1.setField(1,11);
        row1.setField(2,12);

        row2.setField(0,20);
        row2.setField(1,21);
        row2.setField(2,22);

        row3.setField(0,30);
        row3.setField(1,31);
        row3.setField(2,32);


        final DataStream<Row> sourceStream1 = env.fromElements(row1, row2, row3);

        String sqlJoin = "define stream SourceStream1 (id0 string, id1 string, id2 string); " +
                "define stream SourceStream2 (id0 string, id1 string, id2 string); " +
                "FROM SourceStream1 as s1 join SourceStream2 as s2 on s1.id2==s2.id2 " +
                "SELECT id INSERT INTO OutputStream1";

        String sqlGroupBy = "define stream SourceStream1 (id0 string, id1 string, id2 string); " +
                "FROM SourceStream1 SELECT id1 group by id1 INSERT INTO OutputStream1";

        String sqlPlain = "define stream SourceStream1 (id0 string, id1 string, id2 string); " +
                "FROM SourceStream1 SELECT id1 INSERT INTO OutputStream1";

        SiddhiApp siddhiApp = SiddhiCompiler.parse(sqlJoin);


        KeyedStream<Row, String> keyedStream = sourceStream1.keyBy(
                FliddhiPlanner.createFliddhiKeySelector(siddhiApp));

        keyedStream.print();

        env.execute();




        // апи для сидхи, который нужно реализовать
//        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
//        final FliddhiStream outputStream = fEnv.siddhiQL(
//                "FROM SourceStream SELECT id INSERT INTO OutputStream",
//                FliddhiStream.of("SourceStream", sourceStream, "id")
//        );
//
//
//        // стандартный код флинка
//        final List<Integer> actual = outputStream
//                .dataStream()
//                .map(row -> (Integer) row.getField(0))
//                .executeAndCollect(5);
//        assertThat(actual, containsInAnyOrder(1, 2, 3, 4, 5));
    }
}
