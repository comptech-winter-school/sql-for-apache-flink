package ru.comptech2021.fliddhi.selector;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import org.apache.flink.types.Row;

public class FliddhiPlainKeySelector extends FliddhiKeySelector {

    private final String inputStream;

    public FliddhiPlainKeySelector(SiddhiApp siddhiApp) {
        super(siddhiApp);

        Query query = (Query) siddhiApp.getExecutionElementList().get(0);

        inputStream = query.getInputStream().getAllStreamIds().get(0);

        System.out.println("FliddhiPlainKeySelector");
    }

    @Override
    public String getKey(Row row) throws Exception { return inputStream; }
}
