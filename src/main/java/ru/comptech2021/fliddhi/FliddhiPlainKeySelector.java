package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import org.apache.flink.types.Row;

public class FliddhiPlainKeySelector extends FliddhiKeySelector {
    public FliddhiPlainKeySelector(SiddhiApp siddhiApp) {
        super(siddhiApp);
    }

    @Override
    public String getKey(Row row) throws Exception {
        return null;
    }
}
