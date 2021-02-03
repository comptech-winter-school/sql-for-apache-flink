package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;

public abstract class FliddhiKeySelector implements KeySelector<Row, String> {

    protected SiddhiApp siddhiApp;

    public FliddhiKeySelector(SiddhiApp siddhiApp) { //paralelism from inviroment if paralelizm 1 ->..

        this.siddhiApp = siddhiApp;

        System.out.println("FliddhiKeySelector ctor");
    }
}
