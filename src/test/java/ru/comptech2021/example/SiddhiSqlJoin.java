package ru.comptech2021.example;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;

import java.util.Arrays;

/*public class SiddhiFilterExample {*/
public class SiddhiSqlJoin {
    public static void main(String[] args) throws InterruptedException {

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Siddhi Application
        String siddhiApp = "" +

               /* "define stream StockStream (name string, department float, salary long); " +
                "" + // и в третий раз: "зачем это?"
                "@info(name = 'query1') " +
                "from StockStream " +
                "select department, name, salary " +
                "insert into OutputStream;";*/

                /*"define stream StockStream (name string, department float, salary long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream#window.lengthBatch(5) " +
                "" +
                "select  department, min (salary) as minSalary " +
                "group by department "+
                "insert into OutputStream;";*/

                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream TwitterStream (companyID string, tweet string); " +
                /*"define stream ThirdStream (companyID string, tweet string); " +*/
                " " +
                /*"from StockStream#window.time(1 min) as S " +*/
                "from StockStream as S " +
                "left outer " +
                "     join TwitterStream as T " +
                /*  "     join TwitterStream#window.time(2 min) as T " + */
                "    on S.symbol== T.companyID " +
               /* "join ThirdStream#window.time(20 min) as Th " +
                "    on T.companyID== Th.companyID " +*/
                /*"select S.symbol, T.tweet, S.price " + */
                "select S.symbol as symbol, count ( T.tweet) as countTwitter " +
                "group by symbol "+
                "insert into OutputStream ;";


        //Generate runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        //Adding callback to retrieve output events from stream
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                System.out.println(Arrays.toString(events));
                //To convert and print event as a map
                //EventPrinter.print(toMap(events));
            }
        });

        //Get InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");

        //Start processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"00122", 7f, 100L});
        inputHandler.send(new Object[]{"00123", 6f, 200L});
        inputHandler.send(new Object[]{"00124", 5f, 30L});
        inputHandler.send(new Object[]{"00125", 7f, 400L});
        inputHandler.send(new Object[]{"00126", 5f, 50L});
        Thread.sleep(500);

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Get InputHandler to push events into Siddhi
        inputHandler = siddhiAppRuntime.getInputHandler("TwitterStream");

        //Start processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"00122", "aaa"});
        inputHandler.send(new Object[]{"00123", "bbb"});
        inputHandler.send(new Object[]{"00122", "ccc"});
        inputHandler.send(new Object[]{"00128", "ddd"});
        inputHandler.send(new Object[]{"00122", "eee"});
        Thread.sleep(500);

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        /*
        //Get InputHandler to push events into Siddhi
        inputHandler = siddhiAppRuntime.getInputHandler("ThirdStream");

        //Start processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"00121", "aaa"});
        inputHandler.send(new Object[]{"00123", "bbb"});
        inputHandler.send(new Object[]{"00127", "ccc"});
        inputHandler.send(new Object[]{"00128", "ddd"});
        Thread.sleep(500);*/

        //Shutdown runtime
        siddhiAppRuntime.shutdown();


        //Shutdown Siddhi Manager
        siddhiManager.shutdown();

    }
}
