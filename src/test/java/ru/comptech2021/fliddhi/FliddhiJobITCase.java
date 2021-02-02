package ru.comptech2021.fliddhi;


import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.compiler.SiddhiCompiler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FliddhiJobITCase {

    @Test
    public void jobShouldTransferIntegersFromSourceToOutStream() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(1, 2, 3, 4, 5).map(Row::of);


        SiddhiApp siddhiApp = SiddhiCompiler.parse(
                "define stream SourceStream (id string); " +
                        "FROM SourceStream SELECT id group by id INSERT INTO OutputStream1; " +
                        "FROM SourceStream SELECT id group by id INSERT INTO OutputStream2");

        Query query = (Query) siddhiApp.getExecutionElementList().get(0);

        System.out.println(query.getOutputStream().getId());

        System.out.println(query.getInputStream().getAllStreamIds().get(0));

        KeyedStream<Row, String> keyedStream = sourceStream.keyBy(new FliddhiKeySelector(
                "define stream SourceStream (id string); " +
                    "FROM SourceStream SELECT id group by id INSERT INTO OutputStream1"));

        keyedStream.print();

//        final DataStream<Row> sourceStream1 = sourceStream.keyBy(new FliddhiKeySelector(
//                "define stream SourceStream (id string); " +
//                "FROM SourceStream SELECT id group by id INSERT INTO OutputStream1"));
//        sourceStream1.print();


        //Query query = SiddhiCompiler.parseQuery(siddhiApp);

        //System.out.println(query.getOutputStream().getId());

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
