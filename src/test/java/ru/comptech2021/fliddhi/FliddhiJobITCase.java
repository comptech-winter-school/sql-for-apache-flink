package ru.comptech2021.fliddhi;


import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;
import ru.comptech2021.fliddhi.environment.FliddhiExecutionEnvironment;

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
        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("inputStream1", sourceStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(1,
                "FROM SourceStream SELECT id INSERT INTO OutputStream");

/* For FliddhiExecutionOperator testing
        String sql = "FROM SourceStream SELECT id INSERT INTO OutputStream";
        SiddhiManager siddhiManager = new SiddhiManager();
        DataStream<Row> outputStream = sourceStream.transform("", TypeInformation.of(Row.class), new FliddhiExecutionOperator(siddhiManager, sql));
*/

        // стандартный код флинка
        final List<Integer> actual = outputStream
                .get("OutputStream")
                .map(row -> (Integer) row.getField(0))
                .executeAndCollect(5);
        assertThat(actual, containsInAnyOrder(1, 2, 3, 4, 5));
    }
}
