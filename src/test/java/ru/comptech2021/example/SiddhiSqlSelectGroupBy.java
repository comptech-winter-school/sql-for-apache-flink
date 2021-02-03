package ru.comptech2021.example;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;

import java.util.Arrays;




/**
 * Copied from https://siddhi.io/en/v5.1/docs/siddhi-as-a-java-library/
 */
/*public class SiddhiFilterExample {*/
public class SiddhiSqlSelectGroupBy {
    public static void main(String[] args) throws InterruptedException {

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Siddhi Application
        String siddhiApp = "" +
                /*"define stream StockStream (symbol string, price float, volume long); " +  */

               /* "define stream StockStream (name string, department float, salary long); " +
                "" + // и в третий раз: "зачем это?"
                "@info(name = 'query1') " +  // что это?
                "from StockStream " +
                "" +
                "select  department, min (salary) as minsalary " +
                "group by department "+
                "insert into OutputStream;"; */


                "define stream StockStream (name string, department float, salary long); " +
                "" + // и в третий раз: "зачем это?"
                "@info(name = 'query1') " +  // что это?
                "from StockStream " +
                "select department, name, salary " +
                "insert into OutputStream;";

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
        inputHandler.send(new Object[]{"Tom", 7f, 100L});
        inputHandler.send(new Object[]{"Ivan", 6f, 200L});
        inputHandler.send(new Object[]{"Vasya", 5f, 30L});
        inputHandler.send(new Object[]{"Ann", 7f, 400L});
        inputHandler.send(new Object[]{"Bob", 5f, 50L});
        Thread.sleep(500);

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();

    }
}
