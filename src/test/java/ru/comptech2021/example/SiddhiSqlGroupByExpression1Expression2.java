package ru.comptech2021.example;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;

import java.util.Arrays;

public class SiddhiSqlGroupByExpression1Expression2 {


    public static void main(String[] args) throws InterruptedException {

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Siddhi Application
        String siddhiApp = "" +

                // select + group by(exp1, exp2) + window
                "define stream StockStream (name string, number float, grade long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream#window.lengthBatch(27) " +
                "" +
                "select  name, number, avg (grade) as avgGrade " +
                "group by number, name "+
                "insert into OutputStream; ";

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
        inputHandler.send(new Object[]{"физика", 7f, 1L});
        inputHandler.send(new Object[]{"математика", 6f, 2L});
        inputHandler.send(new Object[]{"пение", 5f, 3L});
        inputHandler.send(new Object[]{"пение", 7f, 4L});
        inputHandler.send(new Object[]{"математика", 7f, 4L});
        inputHandler.send(new Object[]{"математика", 7f, 5L});
        inputHandler.send(new Object[]{"изо", 7f, 5L});
        inputHandler.send(new Object[]{"математика", 7f, 5L});
        inputHandler.send(new Object[]{"физика", 7f, 5L});
        inputHandler.send(new Object[]{"физика", 7f, 1L});
        inputHandler.send(new Object[]{"математика", 6f, 2L});
        inputHandler.send(new Object[]{"пение", 7f, 3L});
        inputHandler.send(new Object[]{"пение", 7f, 4L});
        inputHandler.send(new Object[]{"математика", 7f, 4L});
        inputHandler.send(new Object[]{"математика", 7f, 5L});
        inputHandler.send(new Object[]{"изо", 7f, 5L});
        inputHandler.send(new Object[]{"математика", 7f, 5L});
        inputHandler.send(new Object[]{"физика", 7f, 5L});
        inputHandler.send(new Object[]{"физика", 7f, 1L});
        inputHandler.send(new Object[]{"математика", 6f, 2L});
        inputHandler.send(new Object[]{"пение", 6f, 3L});
        inputHandler.send(new Object[]{"пение", 6f, 4L});
        inputHandler.send(new Object[]{"математика", 6f, 4L});
        inputHandler.send(new Object[]{"математика", 6f, 5L});
        inputHandler.send(new Object[]{"изо", 6f, 5L});
        inputHandler.send(new Object[]{"математика", 6f, 5L});
        inputHandler.send(new Object[]{"физика", 6f, 5L});

        Thread.sleep(500);

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();

    }
}

