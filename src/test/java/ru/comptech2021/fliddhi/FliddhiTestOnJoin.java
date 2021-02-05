package ru.comptech2021.fliddhi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;
import ru.comptech2021.fliddhi.environment.FliddhiExecutionEnvironment;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FliddhiTestOnJoin {

    //  outer join
    @Test
    public void testOnOuterJoin1() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"1", 50f, 200L}, new Object[]{"2", 40f, 300L}).map(Row::of);
        final DataStream<Row> twitterStream = env.fromElements(new Object[]{"1", "Hello!"}, new Object[]{"3", "Bye!"}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String queryOuterJoin = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream TwitterStream (companyID string, tweet string); " +
                " " +
                "from StockStream as S " +
                "left outer " +
                // "right outer " +
                "     join TwitterStream as T " +
                "    on S.symbol== T.companyID " +
                "select S.symbol, T.tweet, S.price " +
                "insert into OutputStream ;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        fEnv.registerInputStream("TwitterStream", twitterStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, queryOuterJoin);

        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(1);
        assertThat(actual, contains(Row.of("1",null,50f)));
//        assertThat(actual, contains(Row.of(null,"Hello!")));
    }

    @Test
    public void testOnInnerJoin2() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"1", 50f, 200L}, new Object[]{"2", 40f, 300L}).map(Row::of);
        final DataStream<Row> twitterStream = env.fromElements(new Object[]{"1", "Hello"}, new Object[]{"3", "Bye!"}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String queryInnerJoin = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream TwitterStream (companyID string, tweet string); " +
                " " +
                "from StockStream as S " +
                "     join TwitterStream as T " +
                "    on S.symbol== T.companyID " +
                "select S.symbol, T.tweet, S.price " +
                "insert into OutputStream ;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        fEnv.registerInputStream("TwitterStream", twitterStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, queryInnerJoin);
        System.out.println(outputStream.get("OutputStream"));
        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(1);
        assertThat(actual, contains(Row.of("1", "Hello", 50f)));
    }

    @Test
    public void testOnInnerJoin3() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"1", 50f, 200L}, new Object[]{"2", 40f, 300L}).map(Row::of);
        final DataStream<Row> twitterStream = env.fromElements(new Object[]{"1", "Hello"}, new Object[]{"3", "Bye!"}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String queryInnerJoin = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream TwitterStream (companyID string, tweet string); " +
                " " +
                "from StockStream as S " +
                "     join TwitterStream as T " +
                "select S.symbol, T.tweet, S.price " +
                "insert into OutputStream ;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        fEnv.registerInputStream("TwitterStream", twitterStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, queryInnerJoin);
        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(1);
        assertThat(actual, contains(Row.of("1", "Hello", 50f)));
    }

    @Test
    public void testOnOuterJoin4() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"1", 50f, 200L}, new Object[]{"2", 40f, 300L}).map(Row::of);
        final DataStream<Row> twitterStream = env.fromElements(new Object[]{"1", "Hello"}, new Object[]{"3", "Bye!"}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String queryInnerJoin = ""+
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream TwitterStream (companyID string, tweet string); " +
                " " +
                "from StockStream#window.lengthBatch(3) as S " +
                "left outer " +
                // "right outer " +
                "    join TwitterStream#window.lengthBatch(3) as T " +
                "    on S.symbol== T.companyID " +
                "select S.symbol, T.tweet, S.price " +
                "insert into OutputStream ;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        fEnv.registerInputStream("TwitterStream", twitterStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, queryInnerJoin);
        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(1);
        assertThat(actual, contains(Row.of("1",null,50f)));
//        assertThat(actual, contains(Row.of(null,"Hello!")));
    }

    @Test
    public void testOnOuterJoin5() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"1", 50f, 200L}, new Object[]{"1", 40f, 300L}).map(Row::of);
        final DataStream<Row> twitterStream = env.fromElements(new Object[]{"1", "Hello"}, new Object[]{"3", "Bye!"}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String queryInnerJoin = ""+
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream TwitterStream (companyID string, tweet string); " +
                " " +
                "from StockStream#window.lengthBatch(3) as S " +
                "left outer " +
                "    join TwitterStream#window.lengthBatch(3) as T " +
                "    on S.symbol== T.companyID " +
                "select S.symbol as symbol, count ( T.tweet) as countTwitter " +
                "group by symbol "+
                "insert into OutputStream ;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        fEnv.registerInputStream("TwitterStream", twitterStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, queryInnerJoin);
        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(1);
        assertThat(actual, containsInAnyOrder(Row.of("1",1,50f),Row.of("1",1,40f)));
    }


}
