package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import org.apache.flink.types.Row;

public class FliddhiJoinKeySelector extends FliddhiKeySelector {
    public FliddhiJoinKeySelector(SiddhiApp siddhiApp) {
        super(siddhiApp);

        System.out.println("FliddhiJoinKeySelector ctor");
    }

    @Override
    public String getKey(Row row) throws Exception {
        return row.getField(0).toString(); //заглушка
    }
}
