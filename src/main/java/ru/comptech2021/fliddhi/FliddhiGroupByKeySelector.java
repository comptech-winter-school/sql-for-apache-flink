package ru.comptech2021.fliddhi;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.Query;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FliddhiGroupByKeySelector extends FliddhiKeySelector {

    //private String groupByAttribute;
    private final List<String> groupByAttributes = new ArrayList<>();
    private final String inputStream;

    public FliddhiGroupByKeySelector(SiddhiApp siddhiApp) {
        super(siddhiApp);

        Query query = (Query) siddhiApp.getExecutionElementList().get(0);

        System.out.println("FliddhiGroupByKeySelector ctor");


            for(int i = 0; i < query.getSelector().getGroupByList().size(); i++) {
                groupByAttributes.add(i, query.getSelector().getGroupByList().get(i).getAttributeName());
            }

        //groupByAttribute = query.getSelector().getGroupByList().get(0).getAttributeName();
        inputStream = query.getInputStream().getAllStreamIds().get(0);
    }

    @Override
    public String getKey(Row row) throws Exception {

        StringBuilder stringBuilder = new StringBuilder(inputStream);
        for(String groupByAttribute : groupByAttributes) {
            stringBuilder.append(":");
            stringBuilder.append(Objects.requireNonNull(row.getField(
                    siddhiApp.getStreamDefinitionMap().get(inputStream).
                            getAttributePosition(groupByAttribute))).toString());
        }

        System.out.println(stringBuilder.toString());

        return stringBuilder.toString();


/*
        System.out.println(inputStream + ":" +
                row.getField(
                        siddhiApp.getStreamDefinitionMap()
                                .get(inputStream)
                                .getAttributePosition(groupByAttribute)
                ).toString());

        return inputStream + ":" +
                row.getField(
                siddhiApp.getStreamDefinitionMap()
                        .get(inputStream)
                        .getAttributePosition(groupByAttribute)
                ).toString();

 */
    }
}
