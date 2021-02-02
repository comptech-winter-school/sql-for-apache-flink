package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.compiler.SiddhiCompiler;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;

public class FliddhiKeySelector implements KeySelector<Row, String> {

    private SiddhiApp siddhiApp;
    private String groupByAttribute;
    private String inputStream;

    public FliddhiKeySelector(String sql) {

        siddhiApp = SiddhiCompiler.parse(sql);

        Query query = (Query) siddhiApp.getExecutionElementList().get(0);

        System.out.println("FliddhiKeySelector ctor");

        if(!query.getSelector().getGroupByList().isEmpty()) {
            groupByAttribute = query.getSelector().getGroupByList().get(0).getAttributeName();
            inputStream = query.getInputStream().getAllStreamIds().get(0);
        }
        else {
            throw new UnsupportedOperationException("Group by not found");
        }
    }

    @Override
    public String getKey(Row row) throws Exception {
        System.out.println("FliddhiKeySelector getKey");

        System.out.println((String) row.getField(
                siddhiApp.getStreamDefinitionMap().get(inputStream).getAttributePosition(groupByAttribute)));

        return (String) row.getField(
                siddhiApp.getStreamDefinitionMap().get(inputStream).getAttributePosition(groupByAttribute));
    }
}
