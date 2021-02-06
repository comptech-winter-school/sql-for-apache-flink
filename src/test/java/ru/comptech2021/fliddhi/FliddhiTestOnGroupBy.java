package ru.comptech2021.fliddhi;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import ru.comptech2021.fliddhi.environment.FliddhiExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FliddhiTestOnGroupBy {

    // select + group by + window
    @Test
    public void testOnGBWithWindow1() throws Exception {

        // стандартный код флинка
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"Vasya", 5f, 20L}, new Object[]{"Lena", 5f, 30L}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String querySelectGBWindow = "" +
                "define stream StockStream (name string, department float, salary long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream#window.lengthBatch(2) " +
                "" +
                "select department, min (salary) as minSalary " +
                "group by department "+
                "insert into OutputStream;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, querySelectGBWindow);

        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(2);
//        .print;
//        env.execute();

//                .executeAndCollect(5);
        assertThat(actual, contains(Row.of(5f, 20L)));
    }

    // select
    @Test
    public void testOnSelect1() throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"Vasya", 5f, 50L}, new Object[]{"Lena", 5f, 30L}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String querySelect = "" +
                "define stream StockStream (name string, department float, salary long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select department, name, salary " +
                "insert into OutputStream;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(1, querySelect);


        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(3);
        assertThat(actual, containsInAnyOrder(Row.of(5f,"Vasya", 50L), Row.of(5f,"Lena", 30L)));
    }

    // select + group by
    @Test
    public void testOnGB2() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"Vasya", 5f, 50L}, new Object[]{"Lena", 5f, 30L}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String querySelectGB = "" +
                "define stream StockStream (name string, department float, salary long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "" +
                "select  department, min (salary) as minsalary " +
                "group by department "+
                "insert into OutputStream;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, querySelectGB);

        final List<Row> actual = outputStream.get("OutputStream").executeAndCollect(4);
        System.out.println(actual);
        List<Row> expected = Arrays.asList(Row.of(5f, 50L), Row.of(5f, 30L));

        assertThat(actual, is(expected));
    }

    @Test
    public void testOnGB3() throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStream<Row> sourceStream = env.fromElements(new Object[]{"physics", 4f, 5L}, new Object[]{"math", 6f, 4L},
                new Object[]{"drawing", 7f, 3L},new Object[]{"drawing", 7f, 5L},new Object[]{"math", 6f, 4L},
                new Object[]{"physics", 4f, 3L}).map(Row::of);

        // апи для сидхи, который нужно реализовать
        String querySelectGB = "" +
                "define stream StockStream (name string, number float, grade long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream#window.lengthBatch(27) " +
                "" +
                "select  name, number, avg (grade) as avgGrade " +
                "group by number, name "+
                "insert into OutputStream; ";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("StockStream", sourceStream);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, querySelectGB);


        // стандартный код флинка
        final List<Row> actual = outputStream
                .get("OutputStream")
                //.map(row -> (Integer) row.getField(0))
                .executeAndCollect(4);
        System.out.println(actual);
        assertThat(actual, containsInAnyOrder(Row.of("physics",4f, 4L), Row.of("math",6f, 4L),Row.of("drawing", 7f, 4L)));
    }
    @Test
    public void testOnSelect2() throws Exception {
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
                "" +
                "from SourceStream[volume < 150] " +
                "select symbol, sum(price) as sumPrice group by symbol " +
                "insert into OutputStream;";

        final FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
        fEnv.registerInputStream("SourceStream", sourceStream1);
        final Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(2, query);

        // стандартный код флинка
        final DataStreamSink<Row> actual = outputStream
                .get("OutputStream") //debug here
                .print();

        env.execute();
    }

}
